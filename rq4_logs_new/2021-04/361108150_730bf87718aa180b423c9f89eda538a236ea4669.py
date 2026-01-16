# -*- coding: utf-8 -*-
"""
Created on Sun Apr 18 21:00:33 2021

@author: fewle
"""

import pyrealsense2 as rs
import numpy as np
import cv2
import argparse


parser = argparse.ArgumentParser(description="Read recorded bag file and display depth stream in jet colormap.\
                                Remember to change the stream fps and format to match the recorded.")
parser.add_argument("-i", "--input", type=str, help="Path to the bag file")
args = parser.parse_args()

try:
            pipeline = rs.pipeline()
            config = rs.config()
            rs.config.enable_device_from_file(config,args.input) 

            config.enable_stream(rs.stream.depth, rs.format.z16, 30)
            config.enable_stream(rs.stream.color, rs.format.rgb8, 30)
            profile = pipeline.start(config)

            depth_sensor = profile.get_device().first_depth_sensor()
            depth_scale = depth_sensor.get_depth_scale()
            print("Depth Scale is: " , depth_scale)

            clipping_distance_in_meters = 0.3 #1 meter
            clipping_distance = clipping_distance_in_meters / depth_scale
            
            align_to = rs.stream.color
            align = rs.align(align_to)
            colorizer = rs.colorizer()

    
            while True:
        
                frames = pipeline.wait_for_frames()
                aligned_frames = align.process(frames)
                depth_frame = aligned_frames.get_depth_frame()
                color_frame = aligned_frames.get_color_frame()
                #depth_color_frame = colorizer.colorize(depth_frame)
                
                depth_image = np.asanyarray(depth_frame.get_data())
                color_image = np.asanyarray(color_frame.get_data())

        # remove background
                grey_color = 160
                depth_image_3d = np.dstack((depth_image,depth_image,depth_image)) #depth image is 1 channel, color is 3 channels
                bg_removed = np.where((depth_image_3d > clipping_distance) | (depth_image_3d <= 0), grey_color, color_image)

                def frame_shape(image):
                    width = int(image.shape[0])
                    height = int(image.shape[1])
                    dimensions = (width,height)
                    return dimensions
    

        # 轉換rgb到bgr
                def rgb_to_bgr(image):
                    r,g,b = cv2.split(image)
                    new_image =  cv2.merge([b,g,r])
                    return new_image
        
                color_image = rgb_to_bgr(color_image)
                bg_removed = rgb_to_bgr(bg_removed)

        # Render image in opencv window

                cv2.imshow('Remove background',bg_removed)
                cv2.imshow('Depth stream',depth_image)
                cv2.imshow('RGB stream',color_image)

                key = cv2.waitKey(1)

        
                if key == 27:
                    cv2.destroyAllWindows()

                    break
                if cv2.waitKey(25) & 0xFF == ord('q'):
                    break
finally:
       pass
