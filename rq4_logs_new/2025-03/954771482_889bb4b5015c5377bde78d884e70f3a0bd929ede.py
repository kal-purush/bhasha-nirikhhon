import context

from multicelldetection.classification.classification import  predict, load_a_classifier, visualize_detection_results
from multicelldetection.imagedata.cellcountermakerfile import read_Cell_Counter_Maker_XML_file

import numpy as np
import os
from skimage import io, filters, img_as_float, color, util, morphology
import matplotlib.pyplot as plt


if __name__ == "__main__":
	# #################### use 20x-UC-4-r.tif as testing image
	# step 1 - read RGB TIFF images 
	image_folder = 'inputs//test_1_20x'

	test_image_rgb_1_path = os.path.join(image_folder, '20x-UC-4-rgb.tif') # main image
	test_image_rgb_2_path = os.path.join(image_folder, '20x-UC-4-r.tif')

	test_image_rgb_1 = io.imread(test_image_rgb_1_path)
	test_image_rgb_2 = io.imread(test_image_rgb_2_path)
	
	# Load segmented mask obtained from cellpose
	mask_folder = 'outputs//test_1_20x'
	test_image_rgb_1_mask_filename = os.path.join(mask_folder, '20x-UC-4-rgb-enhanced_cp_masks.png')
	test_image_rgb_1_mask = io.imread(test_image_rgb_1_mask_filename)

	# step 2 - Load a classifier
	classifier_folder = 'outputs//test_1_20x'
	classifier_filepath = os.path.join(classifier_folder, 'RF_classifier_test_7.pkl') 
	
	clf, scaler = load_a_classifier(classifier_filepath)

	# step 3 - get results
	cell_detections = predict(clf, scaler, test_image_rgb_1, test_image_rgb_1_mask, test_image_rgb_2)

	# step 3 - plot results
	fig = visualize_detection_results(test_image_rgb_1, cell_detections)

	output_folder = os.path.join('outputs', os.path.basename(image_folder)) # output folder
	os.makedirs(output_folder, exist_ok=True) # Create the directory if it doesn't exist
	
	figure_filename = os.path.splitext(os.path.basename(test_image_rgb_1_path))[0] + '-prediction.png'
	figure_filepath = os.path.join(output_folder, figure_filename) 

	fig.savefig(figure_filepath, 
            dpi=300,             
            bbox_inches='tight',  
            format='png')         

	plt.close(fig)
