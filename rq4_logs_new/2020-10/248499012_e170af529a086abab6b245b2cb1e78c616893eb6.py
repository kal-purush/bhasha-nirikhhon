# Copyright (c) 2019, NVIDIA CORPORATION.  All rights reserved.
#
# NVIDIA CORPORATION and its licensors retain all intellectual property
# and proprietary rights in and to this software, related documentation
# and any modifications thereto.  Any use, reproduction, disclosure or
# distribution of this software and related documentation without an express
# license agreement from NVIDIA CORPORATION is strictly prohibited.
from __future__ import print_function

import numpy as np
import argparse
import grasp_estimator
import sys
import os
import tensorflow as tf
import glob
import mayavi.mlab as mlab
from visualization_utils import *
import mayavi.mlab as mlab
from grasp_data_reader import regularize_pc_point_count
import rospy
import tf2_ros
from geometry_msgs.msg import Pose, PoseArray,Point, PointStamped, Vector3, Quaternion
from scipy.spatial.transform import Rotation as R
from visualization_msgs.msg import Marker
from std_msgs.msg import Header, ColorRGBA
from sensor_msgs.msg import PointCloud2, PointField, Image, CameraInfo
from panda_simulation.srv import computeGrasps
from cloud_clustering import cloud_clustering
import pcl_helper
#import ros_numpy.point_cloud2 as pc2
import tf as tf_ros
import ros_numpy


def make_parser():
    parser = argparse.ArgumentParser(
        description='6-DoF GraspNet Demo',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--vae_checkpoint_folder',
        type=str, 
        default='checkpoints/latent_size_2_ngpus_1_gan_1_confidence_weight_0.1_npoints_1024_num_grasps_per_object_256_train_evaluator_0_')
    parser.add_argument(
        '--evaluator_checkpoint_folder', 
        type=str, 
        default='checkpoints/npoints_1024_train_evaluator_1_allowed_categories__ngpus_8_/'
    )
    parser.add_argument(
        '--gradient_based_refinement',
        action='store_true',
        default=False,
    )
    parser.add_argument('--npy_folder', type=str, default='demo/data/')
    parser.add_argument('--threshold', type=float, default=0.8)

    return parser


def centeroidnp(arr):
    length = arr.shape[0]
    dims = arr.shape[1]
    centroid = np.zeros(dims)

    for i in range(0 , dims):
        centroid[i] = np.sum(arr[:,i])/length
    return centroid

def get_color_for_pc(pc, K, color_image):
    proj = pc.dot(K.T)
    proj[:, 0] /= proj[:, 2]
    proj[:, 1] /= proj[:, 2]

    pc_colors = np.zeros((pc.shape[0], 3), dtype=np.uint8)
    for i, p in enumerate(proj):
        x = int(p[0])
        y = int(p[1])
        pc_colors[i, :] = color_image[y,x,:]
    
    return pc_colors


def backproject(depth_cv, intrinsic_matrix, extrinsic_matrix, return_finite_depth=True, return_selection=False):

    depth = depth_cv.astype(np.float32, copy=True)

    # get intrinsic matrix
    K = intrinsic_matrix
    Kinv = np.linalg.inv(K)

    # compute the 3D points
    width = depth.shape[1]
    height = depth.shape[0]

    # construct the 2D points matrix
    x, y = np.meshgrid(np.arange(width), np.arange(height))
    ones = np.ones((height, width), dtype=np.float32)
    x2d = np.stack((x, y, ones), axis=2).reshape(width*height, 3)

    # backprojection
    R = np.dot(Kinv, x2d.transpose())

    # compute the 3D points
    X = np.multiply(np.tile(depth.reshape(1, width*height), (3, 1)), R)

    #Transform 3D points with the extrinsic matrix
    X = np.vstack(( X, np.ones(X.shape[1]) ))

    extrinsic_matrix = np.vstack(( extrinsic_matrix, np.array([0, 0, 0, 1]) ))
    ext_inv = np.linalg.inv(extrinsic_matrix)
    print (ext_inv.shape)

    X = np.dot(ext_inv,X)

    X = np.vstack(( np.divide(X[0,:],X[3,:]), np.divide(X[1,:],X[3,:]) , np.divide(X[2,:],X[3,:]) ))

    X = np.array(X).transpose()


    if return_finite_depth:
        selection = np.isfinite(X[:, 0])
        X = X[selection, :]

    if return_selection:
        return X, selection
        
    return X

def transform_poses(poses, transform):

        transform_np = ros_numpy.numpify(transform.transform)

        transformed_poses = [0] * len(poses)
        for i, pose in enumerate(poses):

            #pose_np = ros_numpy.numpify(pose)
            transformed_poses[i] = ros_numpy.msgify(Pose, np.matmul(transform_np, pose))

        return  transformed_poses


## The model is loaded as a global object ##

parser = make_parser()
args = parser.parse_args(sys.argv[1:])
cfg = grasp_estimator.joint_config(
    args.vae_checkpoint_folder,
    args.evaluator_checkpoint_folder,
)
cfg['threshold'] = args.threshold
cfg['sample_based_improvement'] = 1 - int(args.gradient_based_refinement)
cfg['num_refine_steps'] = 10 if args.gradient_based_refinement else 20
estimator = grasp_estimator.GraspEstimator(cfg)
os.environ['CUDA_VISIBLE_DEVICES'] = str(cfg.gpu)
#gpu_options = tf.GPUOptions(per_process_gpu_memory_fraction=0.25)
config = tf.ConfigProto()
#config=tf.ConfigProto(gpu_options=gpu_options)
config.gpu_options.allow_growth = True
sess = tf.Session(config=config)
#sess = tf.Session()
estimator.build_network()
estimator.load_weights(sess)

pub = rospy.Publisher('/grasp_poses', PoseArray )
pub2 = rospy.Publisher("/grasp_points", PointCloud2, queue_size=1000000)
pub3 = rospy.Publisher('/pc_centroid', Marker)

def filter_grasps(grasps_arr):

    grasp_msgs = []
    filter = []
    for i in range(0, grasps_arr.shape[0]):

        r = R.from_dcm([grasps_arr[i , 0:3, 0:3]])
        r_quat = r.as_quat()[0]

        ## Projecting approach axis on camera-planes ##
        ## https://www.euclideanspace.com/maths/geometry/elements/plane/lineOnPlane/index.htm
        r_z = grasps_arr[i , 0:3, 2]                                                        # Third column of Rotation Matrix is the approach axis
        r_proj_XY =  np.cross(np.array([0,0,1]), np.cross( r_z,np.array([0,0,1])) )         # Projection of approach axis on XY Plane - Cross product with z-axis
        r_proj_YZ =  np.cross(np.array([1,0,0]), np.cross( r_z,np.array([1,0,0])) )         # Projection of approach axis on YZ Plane - Cross product with x-axis
        r_proj_XZ =  np.cross(np.array([0,1,0]), np.cross( r_z,np.array([0,1,0])) )         # Projection of approach axis on XZ Plane - Cross product with y-axis

        rad_XY = np.arctan2(r_proj_XY[1], r_proj_XY[0])
        rad_YZ = np.arctan2(r_proj_YZ[2], r_proj_YZ[1])
        rad_XZ = np.arctan2(r_proj_XZ[2], r_proj_XZ[0])

        deg_XY = np.degrees(rad_XY)
        deg_YZ = np.degrees(rad_YZ)
        deg_XZ = np.degrees(rad_XZ)

        r2=R.from_rotvec([0,0,rad_XY])
        r3 = R.from_rotvec([rad_YZ,0,0])

        q2=r2.as_quat()
        q3=r3.as_quat()
        #grasp_msgs = []

        ## Filter the grasps based on the angle of projections of approach axis ##

        '''
            XY is the plane parallel to camera principle axis and contribute to the yaw of grasp.
            We constraint the projection in this plane between -90 and 90 deg. This filters out all
            the grasps that are on the occluded side of object and hence facing towards the camera.

            YZ and XZ are both transverse planes and partially contribute to pitch of grasps. We
            constraint projection with the lesser angle, between -90 and 45 deg. This filters out
            all the grasps that are on the bottom of object i.e; Tilted upwards more than 45 deg.
        '''

        pitch = min(deg_XZ, deg_YZ)
        yaw = deg_XY

        if -90 <= yaw <= 90 and -90 <= pitch <= 45:

                filter.append(i)
                this_grasp = Pose()
                this_grasp.position.x = grasps_arr[i , 0, 3]
                this_grasp.position.y = grasps_arr[i , 1, 3]
                this_grasp.position.z = grasps_arr[i , 2, 3]
                this_grasp.orientation.x = r_quat[0]
                this_grasp.orientation.y = r_quat[1]
                this_grasp.orientation.z = r_quat[2]
                this_grasp.orientation.w = r_quat[3]
                grasp_msgs.append(this_grasp)
    filtered_grasps =  grasps_arr[filter]
    filtered_scores =  grasps_arr[filter]

    return (filtered_grasps, filtered_scores, grasp_msgs)
def main():

    rospy.init_node('grasp_detector', anonymous=True)
    cloud_msg = rospy.wait_for_message('/transformed_cloud_camLink', PointCloud2)
    cloud_arr = ros_numpy.point_cloud2.pointcloud2_to_xyz_array(cloud_msg)


    ## Segment out the points that fit the table plane
    cloud_seg = pcl_helper.plane_seg(cloud_arr)
    cloud_arr = cloud_seg.to_array()
    cloud_colors_arr =100*np.ones((cloud_arr.shape[0],3))

    ## output data info ##
    '''
    print('Total number of points: %s', cloud_arr.shape)
    print('HighestPoint %s', np.nanmax(cloud_arr[:,2]))
    print('LowestPoint %s', np.nanmin(cloud_arr[:,2]))
    print('Farthest Point %s', np.nanmax(cloud_arr[:,0]))
    print('Nearest Point %s', np.nanmin(cloud_arr[:,0]))
    #print('Total number of points above the table: %s', pc.shape)
    '''

    ## Cluster the pointcloud into various target objects ##
    clc = cloud_clustering(cloud_seg)
    clc.euclid_cluster()
    all_grasps = []
    all_scores = []
    all_labels = np.array(([]))

    print(str(len(clc.clusters))+' clusters detected in the input cloud...')

    ## Run inference for each cluster
    for i, cloud in enumerate(clc.clusters):
        points = cloud.to_array()

        ## Grasp-Estimation ##

        print('Estimating Grasps on cluster ' +str(i+1))
        latents = estimator.sample_latents()
        generated_grasps, generated_scores, _ = estimator.predict_grasps(
            sess,
            points,
            latents,
            num_refine_steps=cfg.num_refine_steps,
        )

        print(str(len(generated_scores)) + ' grasps generated')



        scores_np = np.asarray(generated_scores)
        sorting = np.argsort(-scores_np)
        sorted_scores = scores_np[sorting]

        grasps_np = np.asarray(generated_grasps)
        sorted_grasps = grasps_np[sorting]


        all_grasps += sorted_grasps.tolist()          ## Sort grasps for each object
        all_scores += sorted_scores.tolist()
        all_labels = np.concatenate([all_labels, (i+1)*np.ones(sorted_grasps.shape[0])])
        print(all_scores)

    #clc.cluster_mask()
    #clc.cluster_publish()          #Publish clusters as clouds
    print(len(all_scores))
    print(type(all_grasps))

    #mlab.figure(bgcolor=(1,1,1))
    #print(generated_scores)

    ## Sorting all grasps together ##
    scores_np = np.asarray(all_scores)
    sorting = np.argsort(-scores_np)    #Negative sign is for descending-order sorting
    sorted_scores = scores_np[sorting]
    sorted_labels = all_labels[sorting]

    grasps_np = np.asarray(all_grasps)
    sorted_grasps = grasps_np[sorting]
    filtered_grasps, filtered_scores, grasp_msgs = filter_grasps(sorted_grasps)
    #filtered_grasps =  sorted_grasps#[filter]
    #filtered_scores =  sorted_scores#[filter]
    print ('Filtered Grasps: '+str(filtered_scores.shape[0]))

    pub.publish(PoseArray(header=Header(stamp=rospy.Time.now(), frame_id='panda_camera_link'), poses = grasp_msgs))


    if len(all_scores)!=0:
        draw_scene(
                    cloud_arr,
                    pc_color=cloud_colors_arr,
                    grasps= filtered_grasps.tolist(),
                    grasp_scores= filtered_scores.tolist())
    else:
        draw_scene(
                cloud_arr,
                pc_color = cloud_colors_arr
            )



    mlab.show()

def grasp_server():
    rospy.init_node('grasp_detection_server_node')

    while not rospy.is_shutdown():

        '''This node acts as a grasp-server, listening to requests from
        grasp_action_clients. Once this node is initialized, the model
        is loaded to GPU and the grasp-server, stays ready to calculate
        grasp proposals and send them back to clients in decreasing order
        of their scores.'''


        s = rospy.Service('compute_grasps', computeGrasps, compute_grasps_handle)
        rospy.loginfo('compute_grasps service initialized')
        s.spin()



        '''## Loading data from files ##

        ## Only used when ROS-service is not required ##

        print(data.keys())
        depth = data['depth']
        image = data['image']
        K = data['intrinsics_matrix']


        depth = np.load('/home/ahmad3/6dof-graspnet/demo/realsense_data/realsense_depthImg.npy')
	image = np.load('/home/ahmad3/6dof-graspnet/demo/realsense_data/realsense_rgbImg.npy')
        K = np.load('/home/ahmad3/6dof-graspnet/demo/realsense_data/realsense_K.npy')
        R_ext = np.load('/home/ahmad3/6dof-graspnet/demo/realsense_data/realsense_ext.npy')
        '''

## The inference is run within the service handle ##

def compute_grasps_handle(req):
    tf_buffer = tf2_ros.Buffer(rospy.Duration(1200.0))
    tf_listener = tf2_ros.TransformListener(tf_buffer)
    print("Request for Grasp compute recieved by graspnet!!")
    #return AddTwoIntsResponse(req.a + req.b)

    ## Subscribers ##
    #camera_info_topic = '/panda/camera/depth/camera_info'
    #depth_img_topic = '/panda/camera/depth/image_raw'
    #rgb_img_topic ='/panda/camera/color/image_raw'
    cloud_topic = '/transformed_cloud_camLink'

    cloud_msg = rospy.wait_for_message(cloud_topic, PointCloud2)
    pc = ros_numpy.point_cloud2.pointcloud2_to_xyz_array(cloud_msg)
    pc_colors=100*np.ones((pc.shape[0],3))

## output data info ##
    print('Total number of points: %s', pc.shape)
    print('HighestPoint %s', np.nanmax(pc[:,2]))
    print('LowestPoint %s', np.nanmin(pc[:,2]))
    print('Farthest Point %s', np.nanmax(pc[:,0]))
    print('Nearest Point %s', np.nanmin(pc[:,0]))
    #print('Total number of points above the table: %s', pc.shape)

    # Smoothed pc comes from averaging the depth for 10 frames and removing
    # the pixels with jittery depth between those 10 frames.
    #object_pc = data['smoothed_object_pc']

    #points_obj = xyzrgb_array_to_pointcloud2(object_pc, pc_colors, stamp = rospy.Time.now(), frame_id = 'world')
    #pub2.publish(points_obj)
    ## Segment out the points that fit the table plane
    cloud_seg = pcl_helper.plane_seg(pc)
    #cloud_arr = cloud_seg.to_array()
    #cloud_colors_arr =100*np.ones((cloud_arr.shape[0],3))

    clc = cloud_clustering(cloud_seg)
    clc.euclid_cluster()
    cloud = clc.clusters[0]     # Run grasp-detection only on the closest cloud cluster

    points = cloud.to_array()
    cloud_colors =100*np.ones((points.shape[0],3))
    ## Grasp-Estimation ##

    latents = estimator.sample_latents()
    generated_grasps, generated_scores, _ = estimator.predict_grasps(
        sess,
        points,
        latents,
        num_refine_steps=cfg.num_refine_steps,
    )


    ## Sorting grasps ##
    scores_np = np.asarray(generated_scores)
    sorting = np.argsort(-scores_np)    #Negative sign is for descending-order sorting
    sorted_scores = scores_np[sorting]

    grasps_np = np.asarray(generated_grasps)
    sorted_grasps = grasps_np[sorting]
    print('Total generated grasps '+str(len(sorted_grasps.tolist())))
    filtered_grasps, filtered_scores, grasp_msgs = filter_grasps(sorted_grasps)
    print('Filtered grasps '+str(len(filtered_grasps.tolist())))
    try:
        tf_stamped = tf_buffer.lookup_transform('world', 'panda_camera_link' , rospy.Time(0))

    except Exception as inst:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        print('exception: '+str(inst)+' in '+ str(exc_tb.tb_lineno))

    # Transform the grasps to world frame - which is the ref. frame for Panda robot
    transformed_grasps = transform_poses(filtered_grasps.tolist(),tf_stamped)

    for i, grasp in enumerate(transformed_grasps):
        if grasp.position.z < 0.45: #predefined table height
            transformed_grasps.pop(i)

    print('Transformed grasps above the table '+str(len(transformed_grasps)))
    points_obj = pcl_helper.xyzrgb_array_to_pointcloud2(points, cloud_colors, stamp = rospy.Time.now(), frame_id = 'panda_camera_link')
    pub2.publish(points_obj)
    pub.publish(PoseArray(header=Header(stamp=rospy.Time.now(), frame_id='world'), poses = transformed_grasps))
    return PoseArray(header=Header(stamp=rospy.Time.now(), frame_id='world'), poses = transformed_grasps)






if __name__ == '__main__':
    #main()         #Continuously Looks for pointclouds on the input cloud topi, runs inference, displays the grasps in a mayavi plot and repeats
    grasp_server()  #Runs inference on the input cloud topic only when a grasp-request is recieved


    