""" Use a trained YOLOv8n-pose model to run predictions on images. """
#https://docs.ultralytics.com/reference/engine/results/#ultralytics.engine.results.Keypoints

from ultralytics import YOLO
import cv2
import numpy as np
import yolov8_functions
import re
from sklearn.decomposition import PCA
import json

# Dataset path:
dataset_path = "./data/dataset/"
save_path = "./data/predicted"
test_img_path = "/images/test/"
point_names = ['FHC', 'TKC', 'TML', 'FNOC', 'aF1', 'ALL', 'sTMA', 'sFDMA']
landmark_names = ['sTMA1', 'sTMA2', 'FHC', 'sFMDA1', 'sFMDA2','TKC', 'TML', 'FNOC', 'aF1'] # based on labels in config file
imgsize = 3680 # check if the same as trained model
#model_paths = {"ALL" : "./runs/pose/train_ALL_" + str(imgsize) + "_grayscale/weights/best.pt"}
model_paths = {"ALL" : "./runs/pose/train_SGD_"+ str(imgsize) + "_small_batch8/weights/best.pt"}
skipped = []

# PCA script - to check and remove duplicates
predicted_path = "./data/predicted/"
test_path = "./data/dataset/JSON/"
test_images_path =  "./data/dataset/ALL/images/test/"
postprocess_path = "./data/postprocess/"
skipped_path = 'data/postprocess/skipped.json'
save_path = "./data/postprocess"
images_path = "./data/dataset/ALL/images/test/"
landmark_names_pca = ['FHC', 'aF1', 'FNOC', 'TKC', 'sFMDA1', 'sFMDA2', 'sTMA1', 'sTMA2','TML']
point_names_all = ['FHC', 'aF1', 'FNOC', 'TKC', 'sFMDA', 'sTMA', 'TML']
false_prediction = []
image_name = None

# Test points PCA
aF1_points_t = []
fhc_points_t = []
fnoc_points_t = []
tkc_points_t = []
sfdma1_points_t = []
sfdma2_points_t = []
stma1_points_t = []
stma2_points_t = []
tml_points_t = []





# create dataset archive
#yolov8_functions.dataset_archive(save_path)


### PCA learning on test points ###
# get only paths that are to be evaluated from test
json_paths_test = [path for path in yolov8_functions.get_dirs(test_path) if not any(name in path for name in point_names_all)]
json_paths_test = sorted(json_paths_test)

for idx, path in enumerate(json_paths_test):
    skip = False

    # Test points json
    p_names = []
    img_size = []
    test_coordinates = []
    with open(path) as f:
        data = json.load(f)
        for coord in point_names_all:
            if coord == 'sTMA':
                stma1_x = data[coord][0]
                stma1_y = data[coord][1]
                stma2_x = data[coord][2]
                stma2_y = data[coord][3]
                stma1 = [stma1_x, stma1_y]
                stma2 = [stma2_x, stma2_y]
                test_coordinates.append(stma1)
                p_names.append('sTMA1')
                test_coordinates.append(stma2)
                p_names.append('sTMA2')
            elif coord == 'sFMDA':
                stma1_x = data[coord][0]
                stma1_y = data[coord][1]
                stma2_x = data[coord][2]
                stma2_y = data[coord][3]
                stma1 = [stma1_x, stma1_y]
                stma2 = [stma2_x, stma2_y]
                test_coordinates.append(stma1)
                p_names.append('sFMDA1')
                test_coordinates.append(stma2)
                p_names.append('sFMDA2')
            else:
                test_coordinates.append(data[coord])
                p_names.append(coord)
        img_size =  data['Image_size']  # x,y are swapped
        img_size = [img_size[1], img_size[0]]

    # assign points to its evaluation array ['FHC', 'aF1', 'FNOC', 'TKC', 'sFMDA1', 'sFMDA2', 'sTMA1', 'sTMA2','TML']
    fhc_points_t.append(test_coordinates[0])
    aF1_points_t.append(test_coordinates[1])
    fnoc_points_t.append(test_coordinates[2])
    tkc_points_t.append(test_coordinates[3])
    sfdma1_points_t.append(test_coordinates[4])
    sfdma2_points_t.append(test_coordinates[5])
    stma1_points_t.append(test_coordinates[6])
    stma2_points_t.append(test_coordinates[7])
    tml_points_t.append(test_coordinates[8])

# The fit learns some quantities from the data, most importantly the "components" and "explained variance":
pca_fhc = PCA().fit(fhc_points_t)
pca_af1 = PCA().fit(aF1_points_t)
pca_fnoc = PCA().fit(fnoc_points_t)
pca_tkc = PCA().fit(tkc_points_t)
pca_sfdma1 = PCA().fit(sfdma1_points_t)
pca_sfdma2 = PCA().fit(sfdma2_points_t)
pca_stma1 = PCA().fit(stma1_points_t)
pca_stma2 = PCA().fit(stma2_points_t)
pca_tml = PCA().fit(tml_points_t)

pca_arr = [pca_fhc, pca_af1, pca_fnoc, pca_tkc, pca_sfdma1, pca_sfdma2, pca_stma1, pca_stma2, pca_tml]




### Preedictions by YOLO model ###  
directories = yolov8_functions.get_dirs(dataset_path)

# script
for directory in directories:
    print("Directory path:", directory + test_img_path)

    # select correct point name based on directory
    point_name = next((name for name in point_names if name in directory), None)

    if point_name is None:
        continue

    image_paths = yolov8_functions.get_jpg_paths("./" + directory + test_img_path)

    # select correct model based on point
    for img_path in image_paths:
        skip = False
        model_path = model_paths.get(point_name, None)
    
        if model_path is None:
            continue

        # load correct model of yolov8
        yolov8_model = YOLO(model_path)  # load a custom model

        # Run inference on image with arguments - same imgsize as training
        results = yolov8_model.predict(img_path,imgsz=imgsize)  # predict on an image 

        name = yolov8_functions.filename_creation(img_path, ".jpg")
        filename = save_path + "/" + name

        # read image
        img = cv2.imread(img_path)
        temp = np.array(img)
        img_shape = temp.shape
        img_shape = [img_shape[1], img_shape[0]]
        
        # Save JSON file with data
        dictionary = {
            "Image name": name,
            "Image_size": img_shape,
        }
        landmarks = [] # landmarks list
        labels = [] # labels list
        skipped_points = []
        skipped_labels = []
        removed_points = []
        removed_labels = []
        i = 0
        conf_box = []
        conf_pose = []

        for result in results:
            for idx, keypoint in enumerate(result.keypoints):
                point = keypoint.xy.tolist()

                x = point[0][0][0]
                y = point[0][0][1]
                landmark = [x,y]

                # get label and point names from result
                label = int(result.boxes.cls[idx])
                label = landmark_names[label]

                if landmark[0] < img_shape[0]*0.15 or landmark[0] > img_shape[0]*0.85:
                    skipped_points.append(landmark)
                    skipped_labels.append(label)
                    continue

                labels.append(label)
                landmarks.append(landmark)
                #conf_box.append(float(result.boxes.conf[idx]))
                #conf_pose.append(float(result.keypoints.conf[idx]))

        print("Lables:", labels)
        print("Landmarks:", landmarks)

        # PCA noise filtering
 
        # check landmarks for duplicate labels
        duplicates = {x for x in labels if labels.count(x) > 1}
        duplicates = list(duplicates)

        # get array indexes for duplicates
        duplicate_occurances = []
        for dup in duplicates:
            duplicate_occurances = yolov8_functions.get_indices(dup, labels)

        # check landmarks for missing labels
        missing = {x for x in landmark_names if labels.count(x) == 0}
        missing = list(missing)

        duplicate_coords = []
        for idx in duplicate_occurances:
            duplicate_coords.append(landmarks[idx])

        print("Duplicates:", duplicates)
        print("Missing:", missing)
        print("Duplicate indexes:", duplicate_occurances)
        print("Duplicate coordinates:", duplicate_coords)

        # zgoraj funkcije premakni v tole, ker rabim PCA za vsak duplicate ...

        # chose correct PCA method based on duplicate point name
        for dup in duplicates:
            # get array index for PCA choice
            idx = yolov8_functions.get_indices(dup, landmark_names_pca)
            # PCA choice 
            pca = pca_arr[idx[0]]
            # cooridnates for duplicates % 

            """
            components = pca.transform([p])
            filtered = pca.inverse_transform(components)
            print(filtered)
            """

            
        
            print("point indexes:", idx)
            print("Point for PCA:", landmark_names_pca[idx[0]])

        # make PCA prediction
        """
        for idx, p in enumerate(duplicate_coords):
            pca = pca_arr[idx]
            components = pca.transform([p])
            filtered = pca.inverse_transform(components)
            print(filtered)
        """

        """ 
            # sortiranje glede na confidence level
            # check landmarks for duplicates
            if len(labels) > 9:

                duplicates = {x for x in labels if labels.count(x) > 1}
                duplicates = list(duplicates)

                # get array indexes for duplicates
                remove_occurances = []
                for dup in duplicates:
                    occurances = yolov8_functions.get_indices(dup, labels)

                    # get confidence for each duplicate
                    confs = []
                    for idx in occurances:
                        conf = conf_box[idx] + conf_pose[idx]
                        confs.append(conf)
                    
                    # get highest confidence of all duplicates or first highest confidence - upgrade to average
                    highest_conf_idx = confs.index(max(confs))
                    del occurances[highest_conf_idx]

                    for occ in occurances:
                        remove_occurances.append(occ)

                # remove other occurances from labels, landmarks, etc
                remove_occurances.sort(reverse=True)
                for idx in remove_occurances:
                    del labels[idx]
                    del landmarks[idx]
                    del conf_box[idx]
                    del conf_pose[idx]
        
            for idx, landmark in enumerate(landmarks):
                dictionary.update({
                    labels[idx]:landmark,
                })

            dictionary.update({
                    "Skipped points":skipped_points,
                    "Skipped labels":skipped_labels,
                    "Removed points":removed_points,
                    "removed labels":removed_labels
                })

        yolov8_functions.save_prediction_image(landmarks, temp, filename)
        yolov8_functions.create_json_datafile(dictionary, filename)
        """