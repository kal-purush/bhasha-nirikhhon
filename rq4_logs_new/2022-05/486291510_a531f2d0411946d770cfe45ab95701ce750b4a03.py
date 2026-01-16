#from enum import auto

# dataset_url = "http://ballings.co/SVHN.zip"
# data_dir = tf.keras.utils.get_file(origin=dataset_url,
#                                    fname='project_photos',
#                                    untar=True)
# data_dir = pathlib.Path(data_dir)

from importlib.resources import path
from matplotlib.font_manager import json_load
import json
from json import encoder
import os
import tensorflow as tf
import cv2
import pathlib
import numpy as np
import matplotlib.pyplot as plt
import PIL
import PIL.Image
try:
       os.chdir('/Users/Harrison Eller/OneDrive/Desktop/MSBA/Spring 2022/BZAN 554 - Deep Learning/SVHN/train')
except FileNotFoundError:
       os.chdir("G:\\My Drive\\MSBA\\Spring\\Deep Learning\\GA3\\train")
f = open(r'digitStruct.json',)
data = json.load(f)





# os.chdir('C:\\Users\\Harrison Eller\\OneDrive\\Desktop\\MSBA\\Spring 2022\\BZAN 554 - Deep Learning\\SVHN\\train')
# dataset_url = "http://ballings.co/SVHN.zip"
# data_dir = tf.keras.utils.get_file(origin=dataset_url,
#                                    fname='photos',
#                                    untar=True)




try:
       data_dir = pathlib.Path('C:\\Users\\Harrison Eller\\OneDrive\\Desktop\\MSBA\\Spring 2022\\BZAN 554 - Deep Learning\\SVHN')
except FileNotFoundError:
       data_dir = pathlib.Path("G:\\My Drive\\MSBA\\Spring\\Deep Learning\\GA3\\train")
image_count = len(list(data_dir.glob('*/*.png')))
image = list(data_dir.glob('*/*.png'))
print(image_count)


plt.imshow(PIL.Image.open(str(image[2])))
plt.show()


# read image
img = cv2.imread(str(image[0]))
old_image_height, old_image_width, channels = np.max(img.shape)

# create new image of desired size and color (blue) for padding
new_image_width = max(image.wi)
new_image_height = 300
color = (255,0,0)
result = np.full((new_image_height,new_image_width, channels), color, dtype=np.uint8)

# compute center offset
x_center = (new_image_width - old_image_width) // 2
y_center = (new_image_height - old_image_height) // 2

# copy img image into center of result image
result[y_center:y_center+old_image_height, 
       x_center:x_center+old_image_width] = img

# view result
cv2.imshow("result", result)
cv2.waitKey(0)
cv2.destroyAllWindows()



