import csv
import cv2
import numpy as np

lines =[]
with open('./data/driving_log.csv') as csvfile:
  reader = csv.reader(csvfile)
  for line in reader:
    lines.append(line)
    
images = []
measurements =[]

firstline = True
for line in lines:
  if firstline:    #skip first line
      firstline = False
      continue
  source_path = line[0]
  #print(line[0])
  filename = source_path.split('/')[-1]
  current_path = './data/IMG/' + filename
  image = cv2.imread(current_path)
  images.append(image)
  #print(line[3])
  measurement = float(line[3])
  measurements.append(measurement)

augmented_images, augmented_measurements = [], []
for image, measurement in zip(images, measurements):
  augmented_images.append(image)
  augmented_measurements.append(measurement)
  augmented_images.append(cv2.flip(image,1))
  augmented_measurements.append(measurement*-1.0)
  
X_train = np.array(augmented_images)
y_train = np.array(augmented_measurements)

from keras.models import Sequential
from keras.layers import Flatten, Dense, Lambda, Cropping2D

model = Sequential()
model.add(Lambda(lambda x: (x / 255.0) - 0.5 , input_shape = (160, 320, 3) ))
model.add(Cropping2D(cropping=((70,25), (0,0))))
model.add(Flatten())
model.add(Dense(1))

model.compile(loss='mse', optimizer='adam')
model.fit(X_train, y_train, validation_split =0.2, shuffle = True, nb_epoch=2)

model.save('model.h5')
  