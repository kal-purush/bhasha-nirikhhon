# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 11:58:14 2018
@author: jerry
"""

import cv2
import math
import numpy as np
import argparse

def face_orientation(frame, landmarks):
    size = frame.shape #(height, width, color_channel)
    
    #2D image points. reading from txt file
    image_points = np.array([
                            (landmarks[0], landmarks[1]),     # Nose tip
                            (landmarks[2], landmarks[3]),     # Chin
                            (landmarks[4], landmarks[5]),     # Left eye left corner
                            (landmarks[6], landmarks[7]),     # Right eye right corne
                            (landmarks[8], landmarks[9]),     # Left Mouth corner
                            (landmarks[10],landmarks[11])     # Right mouth corner
                        ], dtype="double") 

    # 3D model points.                   
    model_points = np.array([
                            (0.0, 0.0, 0.0),             # Nose tip
                            (0.0, -330.0, -65.0),        # Chin
                            (-225.0, 170.0, -135.0),     # Left eye left corner
                            (225.0, 170.0, -135.0),      # Right eye right corne
                            (-150.0, -150.0, -125.0),    # Left Mouth corner
                            (150.0, -150.0, -125.0)      # Right mouth corner                         
                        ])

    # Camera internals 
    focal_length = size[1]
    center = (size[1]/2, size[0]/2)
    #focal_length = center[0] / np.tan(60/2 * np.pi / 180)
    camera_matrix = np.array(
                         [[focal_length, 0, center[0]],
                         [0, focal_length, center[1]],
                         [0, 0, 1]], dtype = "double"
                         )

    dist_coeffs = np.zeros((4,1)) # Assuming no lens distortion
    #(success, rotation_vector, translation_vector) = cv2.solvePnP(model_points, image_points, camera_matrix, dist_coeffs, flags=cv2.CV_ITERATIVE)
    (success, rotation_vector, translation_vector) = cv2.solvePnP(model_points, image_points, camera_matrix, dist_coeffs, flags=cv2.cv2.SOLVEPNP_ITERATIVE)
    
    #test
    #print "Rotation Vector:\n {0}".format(rotation_vector)
    #print "Translation Vector:\n {0}".format(translation_vector)

    #visalize three axis
    axis = np.float32([[500,0,0], 
                       [0,500,0], 
                       [0,0,500]])                          
    imgpts, jac = cv2.projectPoints(axis, rotation_vector, translation_vector, camera_matrix, dist_coeffs)

    modelpts, jac2 = cv2.projectPoints(model_points, rotation_vector, translation_vector, camera_matrix, dist_coeffs)
    rvec_matrix = cv2.Rodrigues(rotation_vector)[0]

    proj_matrix = np.hstack((rvec_matrix, translation_vector))
    eulerAngles = cv2.decomposeProjectionMatrix(proj_matrix)[6] 
    
    pitch, yaw, roll = [math.radians(_) for _ in eulerAngles]
    pitch = math.degrees(math.asin(math.sin(pitch)))
    roll = -math.degrees(math.asin(math.sin(roll)))
    yaw = math.degrees(math.asin(math.sin(yaw)))

    return imgpts, modelpts, (str(int(roll)), str(int(pitch)), str(int(yaw))), (landmarks[0], landmarks[1])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='face frontalization')
    parser.add_argument('--landmarklist', default='', type=str)
    args = parser.parse_args()
    f = open(args.landmarklist, 'r')
    #f = open('/home/yl/hd/detection/Face-Yaw-Roll-Pitch-from-Pose-Estimation-using-OpenCV/testlandmark.txt','r')

    for line in iter(f):
        img_info = line.split(' ')
        img_path = img_info[0]
        frame = cv2.imread(img_path)
        landmarks =  map(int, img_info[1:])
        
        print img_path
        imgpts, modelpts, rotate_degree, nose = face_orientation(frame, landmarks)
        print "roll/pitch/yaw is {}".format(rotate_degree)
        
        cv2.line(frame, nose, tuple(imgpts[1].ravel()), (0,255,0), 3) #GREEN
        cv2.line(frame, nose, tuple(imgpts[0].ravel()), (255,0,), 3) #BLUE
        cv2.line(frame, nose, tuple(imgpts[2].ravel()), (0,0,255), 3) #RED
        
        remapping = [0,1,2,3,4,5]
        for index in range(len(landmarks)/2):
            random_color = tuple(np.random.random_integers(0,255,size=3)) 
            cv2.circle(frame, (landmarks[index*2], landmarks[index*2+1]), 5, random_color, -1)  
            cv2.circle(frame,  tuple(modelpts[remapping[index]].ravel().astype(int)), 2, random_color, -1)  
                               
        for j in xrange(len(rotate_degree)):
                    cv2.putText(frame, ('{:05.2f}').format(float(rotate_degree[j])), (10, 30 + (50 * j)), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), thickness=2, lineType=2)

        cv2.imwrite('out.jpg', frame)

    f.close()