from importlib.resources import path
from matplotlib.font_manager import json_load
import json
import os
import tensorflow as tf
import cv2
import pathlib
import numpy as np
import matplotlib.pyplot as plt
import PIL
import PIL.Image
from yaml import parse
import pillow

dir = '/Users/Harrison Eller/OneDrive/Desktop/MSBA/Spring 2022/BZAN 554 - Deep Learning/SVHN/train'
os.chdir('/Users/Harrison Eller/OneDrive/Desktop/MSBA/Spring 2022/BZAN 554 - Deep Learning/SVHN/train')
f = open('digitStruct.json',)
data = json.load(f)




## Only Do Once ##
# os.chdir('C:\\Users\\Harrison Eller\\OneDrive\\Desktop\\MSBA\\Spring 2022\\BZAN 554 - Deep Learning\\SVHN\\train')
# dataset_url = "http://ballings.co/SVHN.zip"
# data_dir = tf.keras.utils.get_file(origin=dataset_url,
#                                    fname='photos',
#                                    untar=True)



data_dir = pathlib.Path('C:\\Users\\Harrison Eller\\OneDrive\\Desktop\\MSBA\\Spring 2022\\BZAN 554 - Deep Learning\\SVHN')

image_count = len(list(data_dir.glob('*/*.png')))
image = list(data_dir.glob('*/*.png'))
print(image_count)



plt.imshow(PIL.Image.open(str(image[2])))
plt.show()









for i in range(image_count):
  img = cv2.imread(str(image[i]))
  if i == 0: 
    max_shape = img.shape
  if img.shape[0] > max_shape[0] & img.shape[1] > max_shape[1] :
    max_shape = img.shape




### grayscale and store ###
img = cv2.imread(str(image[0]))
img = cv2.cvtColor(img,cv2.COLOR_BGR2GRAY)
cv2.imshow("result", img)
cv2.waitKey(0)
cv2.destroyAllWindows()
  
# save a image using extension
os.chdir('C:\\Users\\Harrison Eller\\OneDrive\\Desktop\\MSBA\\Spring 2022\\BZAN 554 - Deep Learning\\SVHN\\grayscale')
cv2.imwrite('1.png', img)











# read image
for i in range(image_count):
  img = cv2.imread(str(image[i]))
  
  old_image_height, old_image_width, channels = (img.shape)
  
  if i == 0:
    max_shape = img.shape
  if img.shape > max_shape:
    max_shape = img.shape

  # create new image of desired size and color (blue) for padding
  new_image_width = img.shape[0]
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






  cv2.imshow("result", img)
  cv2.waitKey(0)
  cv2.destroyAllWindows()


class_names = train_ds.class_names
print(class_names)

import matplotlib.pyplot as plt

plt.figure(figsize=(10, 10))
for images, labels in train_ds.take(1):
  for i in range(9):
    ax = plt.subplot(3, 3, i + 1)
    plt.imshow(images[i].numpy().astype("uint8"))
    plt.title(class_names[labels[i]])
    plt.axis("off")

plt.show()