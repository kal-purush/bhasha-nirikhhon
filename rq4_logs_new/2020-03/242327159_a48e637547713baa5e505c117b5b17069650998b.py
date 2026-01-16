#!/usr/bin/env python
# coding: utf-8

# In[1]:


import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import gdal

#custom files import

import skimage
import functions as fu


year = '_2018/'
#year = '_2019/'
data_folder_path = '/home/chris/Desktop/diploma/Diploma-Workspace/Data' + year
labels_folder_path = '/home/chris/Desktop/diploma/Diploma-Workspace/Ground_Truth_Data' + year
shapefile_folder = 'Shapefiles/'
crop_masks_folder = 'Crop_Masks/'

cropped_data ='cropped_data/'


#IMPORT LABELS

crop_name = ['Cotton', 'Corn', 'Peanuts']
mask_suffix = '_mask.tif'
#Read as TIF
# crop_data_tif = gdal.Open(labels_folder_path + 'CDL.tif')
#crop_mask_tif = gdal.Open(labels_folder_path + 'CMASK.tif')
crops_only_tif = gdal.Open(labels_folder_path + 'CDL_CROPS_ONLY.tif')

cotton_tif = gdal.Open(labels_folder_path + crop_name[0] + mask_suffix)
#fallow_2_tif = gdal.Open(labels_folder_path + 'CDL_FALLOW_2.tif')
peanuts_tif = gdal.Open(labels_folder_path + crop_name[2] + mask_suffix)
#other_hay_4_tif = gdal.Open(labels_folder_path + 'CDL_OTHER_HAY_NO_ALFALA_4.tif')
corn_tif = gdal.Open(labels_folder_path + crop_name[1] + mask_suffix)

crops_tifs = [cotton_tif, corn_tif, peanuts_tif]

#Read as Arrays
# crop_data = np.array(crop_data_tif.GetRasterBand(1).ReadAsArray())# crop_data_tif.read(1).astype('float64')
# crop_mask = np.array(crop_mask_tif.GetRasterBand(1).ReadAsArray()) #crop_mask_tif.read(1).astype('float64')
crops_only = np.array(crops_only_tif.GetRasterBand(1).ReadAsArray()) #crops_only_tif.read(1).astype('float64')

cotton_array = np.array(cotton_tif.GetRasterBand(1).ReadAsArray()) #cotton_1_tif.read(1).astype('float64')
#fallow_2 = np.array(fallow_2_tif.GetRasterBand(1).ReadAsArray()) #fallow_2_tif.read(1).astype('float64')
peanuts_array = np.array(peanuts_tif.GetRasterBand(1).ReadAsArray()) #peanuts_3_tif.read(1).astype('float64')
#other_hay_4 = np.array(other_hay_4_tif.GetRasterBand(1).ReadAsArray()) #other_hay_4_tif.read(1).astype('float64')
corn_array = np.array(corn_tif.GetRasterBand(1).ReadAsArray()) #corn_5_tif.read(1).astype('float64')

crops_arrays_list = [cotton_array, corn_array, peanuts_array]

#read csv with crops type
labels = pd.read_csv(labels_folder_path + 'CDL_data.tif.vat.csv')
labels = labels.rename(columns=lambda x: x.strip())
existing_labels = pd.read_csv(labels_folder_path + 'cdl_existing_labels.csv')
existing_labels = existing_labels.rename(columns=lambda x: x.strip())
# existing_crop_labels = pd.read_csv(labels_folder_path + 'cdl_existing_crop_labels.csv')
# existing_crop_labels = existing_labels.rename(columns=lambda x: x.strip())

idx1 = (pd.Index(existing_labels['Value'])).union([0])
idx2 = np.unique(crops_only)

# print((idx1))
# print((idx2))

all_existing_labels = labels[labels['VALUE'].isin(idx1)]
crop_existing_labels = labels[labels['VALUE'].isin(idx2)]

# print(all_existing_labels)
# print(crop_existing_labels)


# In[2]:


parcel_list_1 = []
parcel_list_2 = []

threshold = int(input("Enter threshold of number of pixels per parcel. (press enter for default=200): "))
if threshold=='':
    threshold = 200    


for testing in crops_arrays_list:
    opened_image_1 = skimage.morphology.area_opening(testing,connectivity=1,area_threshold=threshold)
    opened_image_2 = skimage.morphology.area_opening(testing,connectivity=2,area_threshold=threshold)

    #binary_opened_image = skimage.morphology.binary_opening(mask)

    #closed_image = skimage.morphology.area_closing(testing,connectivity=2,area_threshold=256)
    #binary_closed_image = skimage.morphology.binary_closing(binary_opened_image)

    #opening_followed_by_closing = skimage.morphology.area_closing(opened_image,connectivity=2,area_threshold=256)
    parcel_list_1.append(skimage.morphology.area_closing(opened_image_1,connectivity=1,area_threshold=threshold))
    parcel_list_2.append(skimage.morphology.area_closing(opened_image_2,connectivity=2,area_threshold=threshold))

    
#summed_map = parcel_list[0] + parcel_list[1] + parcel_list[2]    
# plt.figure(figsize=(50,20))
# plot.show(summed_map)
for idx,crop in enumerate(parcel_list_1):
    plt.figure(figsize=(50,20))
    plt.imshow(crops_arrays_list[idx])
    plt.figure(figsize=(50,20))
    plt.imshow(crop)
    plt.figure(figsize=(50,20))
    plt.imshow(parcel_list_2[idx])
    plt.show()



# In[4]:


save_shapefiles = input("Save Shapefiles? (y/n)? ")
if save_shapefiles=='n':
    sys.exit(0)

#4-way connectivity
parcel_list = parcel_list_1
#8-way connectivity
#parcel_list = parcel_list_2
for idx,crop in enumerate(crop_name):
    temp = fu.array_to_raster(parcel_list[idx], crops_tifs[idx], data_folder_path + crop_masks_folder + crop + '_parcel_mask_' + str(threshold))
    temp = fu.raster_to_vector_polygonize(temp, data_folder_path + shapefile_folder, crop + '_shapefile_'+ str(threshold))
