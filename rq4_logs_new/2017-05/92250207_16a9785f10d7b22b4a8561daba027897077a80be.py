import argparse
import base64
from datetime import datetime
import os
import shutil

import numpy as np
import socketio
import eventlet
import eventlet.wsgi
from PIL import Image
from flask import Flask
from io import BytesIO


import cv2
import math


import h5py
from keras.models import load_model
from keras.models import model_from_json
from keras.models import Sequential, Model
from keras.initializers import VarianceScaling, Zeros
from keras.layers import Convolution2D, Flatten, MaxPooling2D, Lambda, ELU, Conv2D
from keras.layers.core import Dense, Dropout, Activation
from keras.optimizers import Adam
from keras.callbacks import Callback
from keras.layers.normalization import BatchNormalization
from keras.regularizers import l2
import model


sio = socketio.Server()
app = Flask(__name__)
nmodel = None
prev_image_array = None


#new_size_col,new_size_row = 64, 64

def crop_and_resize_image(img):
    shape = img.shape
    img = img[math.floor(shape[0]/5):shape[0]-25, 0:shape[1]]
    img = cv2.resize(img, (PROCESSED_IMG_COLS, PROCESSED_IMG_ROWS), interpolation=cv2.INTER_AREA)    
    img = cv2.cvtColor(img, cv2.COLOR_RGB2HSV)
    return img
    
    
@sio.on('telemetry')
def telemetry(sid, data):
    if data:
        # The current steering angle of the car
        steering_angle = data["steering_angle"]
        # The current throttle of the car
        throttle = data["throttle"]
        # The current speed of the car
        speed = data["speed"]
        # The current image from the center camera of the car
        imgString = data["image"]
        image = Image.open(BytesIO(base64.b64decode(imgString)))
        image_array = np.asarray(image)
        #steering_angle = float(nmodel.predict(image_array[None, :, :, :], batch_size=1))
        
        image_array = crop_and_resize_image(image_array)
        transformed_image_array = image_array[None, :, :, :]
        # This model currently assumes that the features of the model are just the images. Feel free to change this.
        steering_angle = 1.12*float(nmodel.predict(transformed_image_array, batch_size=1))
        
        min_speed = 5 
        max_speed = 12 
        if float(speed) < min_speed:
            throttle = 2.0
        elif float(speed) > max_speed:
            throttle = -1.0
        else:
            throttle = 0.15
            
        print(steering_angle, speed, throttle)
        send_control(steering_angle, throttle)

        # save frame
        if args.image_folder != '':
            timestamp = datetime.utcnow().strftime('%Y_%m_%d_%H_%M_%S_%f')[:-3]
            image_filename = os.path.join(args.image_folder, timestamp)
            image.save('{}.jpg'.format(image_filename))
    else:
        # NOTE: DON'T EDIT THIS.
        sio.emit('manual', data={}, skip_sid=True)


@sio.on('connect')
def connect(sid, environ):
    print("connect ", sid)
    send_control(0, 0)


def send_control(steering_angle, throttle):
    sio.emit(
        "steer",
        data={
            'steering_angle': steering_angle.__str__(),
            'throttle': throttle.__str__()
        },
        skip_sid=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Remote Driving')
    parser.add_argument(
        'model',
        type=str,
        help='Path to model h5 file. Model should be on the same path.'
    )
    parser.add_argument(
        'image_folder',
        type=str,
        nargs='?',
        default='',
        help='Path to image folder. This is where the images from the run will be saved.'
    )
    args = parser.parse_args()

	# There was problem in loading model after upgrading to Keras 2.0. Using the weights from the saved model and loading the definition separately
	
    #model = load_model(args.model)
    
    # load json and create model
	# json_file = open(args.model +".json", 'r')
	# loaded_model_json = json_file.read()
	# json_file.close()
	# model = model_from_json(loaded_model_json)    
	# model.compile('adam', 'mse')

    

    # processed image variables
    PROCESSED_IMG_ROWS = 66 #160 #64
    PROCESSED_IMG_COLS = 200 #320 #64
    PROCESSED_IMG_CHANNELS = 3
	
    nmodel = model.build_model(PROCESSED_IMG_ROWS, PROCESSED_IMG_COLS, PROCESSED_IMG_CHANNELS)

    # load weights into new model
    nmodel.load_weights(args.model + ".h5")
    #nmodel.load_weights(os.path.join(os.path.dirname(modelFile), 'model.h5'))
    print("Loaded model from disk")

    #args.image_folder = './run1'
    if args.image_folder != '':
        print("Creating image folder at {}".format(args.image_folder))
        if not os.path.exists(args.image_folder):
            os.makedirs(args.image_folder)
        else:
            shutil.rmtree(args.image_folder)
            os.makedirs(args.image_folder)
        print("RECORDING THIS RUN ...")
    else:
        print("NOT RECORDING THIS RUN ...")
 
    # wrap Flask application with engineio's middleware
    app = socketio.Middleware(sio, app)

    # deploy as an eventlet WSGI server
    eventlet.wsgi.server(eventlet.listen(('', 4567)), app)