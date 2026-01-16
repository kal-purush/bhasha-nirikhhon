#! /usr/bin/env python

from optparse import OptionParser
import json
from pprint import pprint
import cv2
import os
import re
import numpy as np

if __name__ == '__main__':
    parser = OptionParser()
    # Read the options
    parser.add_option("", "--train-annotation", dest="train_annotation_file", default="", help="frame level training annotation JSON file")
    parser.add_option("", "--train-annotation-list", dest="train_annotation_list", default="", help="list of frame level training annotation JSON files")
    parser.add_option("", "--test-annotation", dest="test_annotation_file", default="", help="frame level testing annotation JSON file")
    parser.add_option("", "--project-path", dest="project_dir", default="", help="path containing data directory")
    parser.add_option("", "--mh-neighborhood", dest="mh_neighborhood", type="int", default=10, help="distance from mouth hook for a keyppoint to be considered relevant for training")
    parser.add_option("", "--desc-dist-threshold", dest="desc_distance_threshold", type="float", default=0.1, help="threhsold on distance between test descriptor and its training nearest neighbor to count its vote")
    parser.add_option("", "--vote-patch-size", dest="vote_patch_size", type="int", default=15, help="half dimension of the patch within which each test descriptor casts a vote, the actual patch size is 2s+1 x 2s+1")
    parser.add_option("", "--vote-sigma", dest="vote_sigma", type="float", default=3.0, help="spatial sigma spread of a vote within the voting patch")
    parser.add_option("", "--outlier-error-dist", dest="outlier_error_dist", type="int", default=15, help="distance beyond which errors are considered outliers when computing average stats")
    parser.add_option("", "--display", dest="display_level", default=0, type="int", help="display intermediate and final results visually, level 5 for all, level 1 for final, level 0 for none")
    
    (options, args) = parser.parse_args()
    
    print "annotation_file:" , options.train_annotation_file
    
    if (options.train_annotation_file != ""):
        with open(options.train_annotation_file) as fin_annotation:
            train_annotation = json.load(fin_annotation)
    else:
        train_annotation = {}
        train_annotation["Annotations"] = []
        with open(options.train_annotation_list) as fin_annotation_list:
            for train_annotation_file in fin_annotation_list:
                train_annotation_file = os.path.join(options.project_dir, re.sub(".*/data/", "data/", train_annotation_file.strip()))
                with open(train_annotation_file) as fin_annotation:
                    tmp_train_annotation = json.load(fin_annotation)
                    train_annotation["Annotations"].extend(tmp_train_annotation["Annotations"])
    
    with open(options.test_annotation_file) as fin_annotation:
        test_annotation = json.load(fin_annotation)
    # Histogram Equalisation Object
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
# SURF Detector Object
    surf = cv2.SURF(600, nOctaves=3, nOctaveLayers=3)
    
    rmh_kp_train = []
    rmh_desc_train = []
    rmh_vote_train = []
    
    for i in range(0, len(train_annotation["Annotations"])):
        frame_file = train_annotation["Annotations"][i]["FrameFile"]
        frame_file = re.sub(".*/data/", "data/", frame_file)
        frame_file = os.path.join(options.project_dir , frame_file)
        print frame_file
        # Read Frame
        frame = cv2.imread(frame_file)
        
        if(options.display_level >= 2):
            display_frame = frame.copy()
        else:
            display_frame = None
        
        rmh_coords = None
        for j in range(0, len(train_annotation["Annotations"][i]["FrameValueCoordinates"])):
            if (train_annotation["Annotations"][i]["FrameValueCoordinates"][j]["Name"] == "RightMHhook"):
                rmh_coords = {}
                rmh_coords["x"] = int(train_annotation["Annotations"][i]["FrameValueCoordinates"][j]["Value"]["x_coordinate"])
                rmh_coords["y"] = int(train_annotation["Annotations"][i]["FrameValueCoordinates"][j]["Value"]["y_coordinate"])
        
        if (rmh_coords is not None):
            rmh_kp, rmh_desc = surf.detectAndCompute(frame, None)
            for k in range(0, len(rmh_kp)):
                x,y = rmh_kp[k].pt
                a = np.pi * rmh_kp[k].angle/180.0
                # if key point is less than certain distance from the
                if(np.sqrt(np.square(x - rmh_coords["x"]) + np.square(y - rmh_coords["y"])) <= options.mh_neighborhood):
                    rmh_kp_train.append(rmh_kp[k])
                    rmh_desc_train.append(rmh_desc[k])
                    rmh_dp = np.array([rmh_coords["x"] - x, rmh_coords["y"] - y]).T
                    rmh_R = np.array([[np.cos(a), -np.sin(a)], [np.sin(a), np.cos(a)]]).T
                    rmh_dp_R = np.dot(rmh_R, rmh_dp)
                    rmh_vote_train.append(rmh_dp_R)
            
            if(display_frame != None):
                cv2.circle(display_frame, (rmh_coords["x"], rmh_coords["y"]), 4, (0, 0, 255), thickness=-1)
                cv2.circle(display_frame, (rmh_coords["x"], rmh_coords["y"]), options.mh_neighborhood, (0, 255, 255), thickness=3)
        
        if(display_frame != None):
            display_frame = cv2.resize(display_frame, (0,0), fx=0.5, fy=0.5)
            cv2.imshow("mouth hook annotation", display_frame)
    
    print len(rmh_kp_train)
    rmh_knn = cv2.KNearest()
    rmh_desc_train_samples = np.array(rmh_desc_train)
    rmh_kp_train_responses = np.arange(len(rmh_kp_train),dtype = np.float32)
    rmh_knn.train(rmh_desc_train_samples,rmh_kp_train_responses)
    
    rmh_vote = np.zeros((2*options.vote_patch_size+1, 2*options.vote_patch_size+1, 1), np.float)
    for x in range(-options.vote_patch_size, options.vote_patch_size+1):
        for y in range(-options.vote_patch_size, options.vote_patch_size+1):
            rmh_vote[y+options.vote_patch_size,x+options.vote_patch_size] = 1.0 + np.exp(-0.5*(x*x+y*y)/(np.square(options.vote_sigma))) / (options.vote_sigma*np.sqrt(2*np.pi))
    
    rmh_error_dists = []
    rmh_n_outlier_error_dist = 0
    rmh_n_eval = 0
    
    for i in range(0, len(test_annotation["Annotations"])):
        frame_file = test_annotation["Annotations"][i]["FrameFile"]
        frame_file = re.sub(".*/data/", "data/", frame_file)
        frame_file = os.path.join(options.project_dir , frame_file)
        print frame_file
        
        frame = cv2.imread(frame_file)
        
        if(options.display_level >= 2):
            rmh_display_voters = frame.copy()
        
        rmh_coords_gt = None
        for j in range(0, len(test_annotation["Annotations"][i]["FrameValueCoordinates"])):
            if (test_annotation["Annotations"][i]["FrameValueCoordinates"][j]["Name"] == "RightMHhook"):
                rmh_coords_gt = {}
                rmh_coords_gt["x"] = int(test_annotation["Annotations"][i]["FrameValueCoordinates"][j]["Value"]["x_coordinate"])
                rmh_coords_gt["y"] = int(test_annotation["Annotations"][i]["FrameValueCoordinates"][j]["Value"]["y_coordinate"])
        
        rmh_vote_map = np.zeros((np.shape(frame)[0], np.shape(frame)[1], 1), np.float)
        
        kp_frame, desc_frame = surf.detectAndCompute(frame, None)
        for h, desc in enumerate(desc_frame):
            desc = np.array(desc,np.float32).reshape((1,128))
            rmh_retval, rmh_results, rmh_neigh_resp, rmh_dists = rmh_knn.find_nearest(desc,1)
            r,d =  int(rmh_results[0][0]),rmh_dists[0][0]
            if (d <= options.desc_distance_threshold):
                rmh_a = np.pi * kp_frame[h].angle / 180.0
                rmh_R = np.array([[np.cos(a), -np.sin(a)], [np.sin(a), np.cos(a)]])
                rmh_p = kp_frame[h].pt + np.dot(rmh_R, rmh_vote_train[r])
                x,y = rmh_p
                if (not(x <= options.vote_patch_size or x >= np.shape(frame)[1]-options.vote_patch_size or y <= options.vote_patch_size or y >= np.shape(frame)[0]-options.vote_patch_size)):
                    rmh_vote_map[y-options.vote_patch_size:y+options.vote_patch_size+1, x-options.vote_patch_size:x+options.vote_patch_size+1] += rmh_vote
                    if (options.display_level >= 2):
                        cv2.circle(rmh_display_voters, (int(x), int(y)), 4, (0, 0, 255), thickness=-1)
        
        if (options.display_level >= 2):
            rmh_display_voters = cv2.resize(rmh_display_voters, (0,0), fx=0.5, fy=0.5)
            cv2.imshow("voters", rmh_display_voters)
        
        rmh_vote_max = np.amax(rmh_vote_map)
        rmh_vote_map /= rmh_vote_max
        rmh_vote_max_loc = np.where(rmh_vote_map == np.amax(rmh_vote_map))
        rmh_coords_est = {}
        rmh_coords_est["x"] = int(rmh_vote_max_loc[1])
        rmh_coords_est["y"] = int(rmh_vote_max_loc[0])
        if (rmh_coords_gt is not None):
            rmh_error_dist = np.sqrt(np.square(rmh_coords_gt["x"] - rmh_coords_est["x"]) + np.square(rmh_coords_gt["y"] - rmh_coords_est["y"]))
            print "Distance between annotated and estimated MH location:", rmh_error_dist
            rmh_n_eval += 1
            if (rmh_error_dist <= options.outlier_error_dist):
                rmh_error_dists.append(rmh_error_dist)
            else:
                rmh_n_outlier_error_dist += 1
        
        if (options.display_level >= 1):
            rmh_display_vote_map = np.array(frame.copy(), np.float)
            rmh_display_vote_map /= 255.0
            rmh_display_vote_map[:, :, 2] = rmh_vote_map[:, :, 0]
            cv2.circle(rmh_display_vote_map, (rmh_coords_est["x"], rmh_coords_est["y"]), 4, (0, 255, 255), thickness=-1)
            rmh_display_vote_map = cv2.resize(rmh_display_vote_map, (0,0), fx=0.5, fy=0.5)
            cv2.imshow("voters", rmh_display_vote_map)
        
        if (options.display_level >= 1):
            key_press = cv2.waitKey(-1)
            if (key_press == 113 or key_press == 13):
                break
    
    print os.sys.argv
    print "Number of outlier error distances (beyond %d) = %d / %d = %g" % ( options.outlier_error_dist, rmh_n_outlier_error_dist, rmh_n_eval, float(rmh_n_outlier_error_dist) / float(rmh_n_eval) )
    print "Median inlier error dist =", np.median(rmh_error_dists)
    print "Mean inlier error dist =", np.mean(rmh_error_dists)
    
    cv2.destroyWindow("frame")