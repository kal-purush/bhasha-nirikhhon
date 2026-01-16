#!/usr/bin/python
# coding: utf-8

import sys
from plantcv import plantcv as pcv
import cv2
import numpy as np
import argparse
import string
from matplotlib import pyplot as plt
from scipy.ndimage import measurements



### Parse command-line arguments
def options():
    parser = argparse.ArgumentParser(description="Imaging processing with opencv")
    parser.add_argument("-i", "--image", help="Input image file.", required=True)
    parser.add_argument("-o", "--outdir", help="Output directory for image files.", required=False)
    parser.add_argument("-r","--result", help="result file.", required= False )
    parser.add_argument("-W","--writeimg", help="write out images.", default=True)
    parser.add_argument("-D", "--debug", help="can be set to 'print' or 'none' (or 'plot' if in jupyter) prints intermediate images.", default=None)
    args = parser.parse_args()
    return args

### Main pipeline
def main():
    # Get options
    args = options()

    pcv.params.debug=args.debug
    pcv.params.debug_outdir=args.outdir

    # Read image
    img, path, filename = pcv.readimage(args.image)

    # Convert RGB to HSV and extract the Saturation channel
    b = pcv.rgb2gray_hsv(img, 's')

    # Threshold the Saturation image
    b_thresh = pcv.threshold.binary(b, 70, 255, 'light')

    #Create a mask of the thesholded image over the original image
    masked = pcv.apply_mask(img, b_thresh, 'white')

    # Identify objects
    id_objects, obj_hierarchy = pcv.find_objects(masked, b_thresh)

    # Define region of interest
    roi1, roi_hierarchy = pcv.roi.rectangle(x=1700, y=100, h=3200, w=1600, img=masked)

    # Decide which objects to keep
    roi_objects, hierarchy3, kept_mask, obj_area = pcv.roi_objects(img, 'partial', roi1, roi_hierarchy, id_objects,
                                                                   obj_hierarchy)

    # Object combine kept objects
    obj, mask = pcv.object_composition(img, roi_objects, hierarchy3)

############### Analysis ################

    outfile=False
    if args.writeimg==True:
        outfile=args.outdir+"/"+filename

    # Find shape properties, output shape image (optional)
    shape_header, shape_data, shape_img = pcv.analyze_object(img, obj, mask, args.outdir + '/' + filename)

    # Shape properties relative to user boundary line (optional)
    #boundary_header, boundary_data, boundary_img1 = pcv.analyze_bound_horizontal(img, obj, mask, 1680, args.outdir + '/' + filename)

    # Determine color properties: Histograms, Color Slices and Pseudocolored Images, output color analyzed images (optional)
    color_header, color_data, color_img = pcv.analyze_color(img, kept_mask, 256, None, 'v', 'img', args.outdir + '/' + filename)

    # Write shape and color data to results file
    result=open(args.result,"a")
    result.write('\t'.join(map(str,shape_header)))
    result.write("\n")
    result.write('\t'.join(map(str,shape_data)))
    result.write("\n")
    for row in shape_img:
        result.write('\t'.join(map(str,row)))
        result.write("\n")
    result.write('\t'.join(map(str,color_header)))
    result.write("\n")
    result.write('\t'.join(map(str,color_data)))
    result.write("\n")
    for row in color_img:
        result.write('\t'.join(map(str,row)))
        result.write("\n")
    result.close()

if __name__ == '__main__':
    main()