# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 17:21:38 2019

@author: rodrigo.ferrari
"""

# check version of keras_vggface
import keras_vggface
# print version
print(keras_vggface.__version__)


# confirm mtcnn was installed correctly
import mtcnn
# print version
print(mtcnn.__version__)

import os
from numpy import expand_dims
from matplotlib import pyplot
from PIL import Image
from numpy import asarray
from mtcnn.mtcnn import MTCNN
from keras_vggface.vggface import VGGFace
from keras_vggface.utils import preprocess_input
from keras_vggface.utils import decode_predictions
from scipy.spatial.distance import cosine
import cv2

# extract a single face from a given photograph
def extract_face(filename, required_size=(224, 224)):
	# load image from file
	pixels = pyplot.imread(filename)
	# create the detector, using default weights
	detector = MTCNN()
	# detect faces in the image
	results = detector.detect_faces(pixels)
	# extract the bounding box from the first face
	x1, y1, width, height = results[0]['box']
	x2, y2 = x1 + width, y1 + height
	# extract the face
	face = pixels[y1:y2, x1:x2]
	# resize pixels to the model size
	image = Image.fromarray(face)
	image = image.resize(required_size)
	face_array = asarray(image)
	return face_array

# extract faces and calculate face embeddings for a list of photo files
def get_embeddings(filenames,crop=True):
    if crop==True:
        # extract faces
        faces = [extract_face(f) for f in filenames]
    else:
        faces = [pyplot.imread(f) for f in filenames]
        for i in range(0,len(faces)):
            faces[i] = cv2.resize(faces[i], (224, 224))
	# convert into an array of samples
    samples = asarray(faces, 'float32')
	# prepare the face for the model, e.g. center pixels
    samples = preprocess_input(samples, version=2)
	# create a vggface model
    model = VGGFace(model='resnet50', include_top=False, input_shape=(224, 224, 3), pooling='avg')
	# perform prediction
    yhat = model.predict(samples)
    return yhat

# determine if a candidate face is a match for a known face
def is_match(known_embedding, candidate_embedding, thresh=0.5):
	# calculate distance between embeddings
	score = cosine(known_embedding, candidate_embedding)
	if score <= thresh:
		print('>face is a Match (%.3f <= %.3f)' % (score, thresh))
	else:
		print('>face is NOT a Match (%.3f > %.3f)' % (score, thresh))

# define filenames
filenames = ['Aaron Paul1.jpg', 'Aaron Paul2.jpg',
	'Aaron Paul3.jpg', 'Aaron Paul4.jpg', 'anne hathaway0.jpg',
    'anne hathaway1.jpg','Benedict Cumberbatch0.jpg',
    'Benedict Cumberbatch1.jpg','grant gustin face0.jpg',
    'grant gustin face1.jpg']
# get embeddings file filenames
embeddings = get_embeddings(filenames,crop=False)
# define Aaron Paul
ids = embeddings[0]
# verify known photos of sharon
print('Positive Tests')
is_match(embeddings[0], embeddings[1])
is_match(embeddings[0], embeddings[2])
is_match(embeddings[0], embeddings[3])
# verify known photos of other people
print('Negative Tests')
is_match(embeddings[0], embeddings[4])
is_match(embeddings[0], embeddings[5])
is_match(embeddings[0], embeddings[6])
is_match(embeddings[0], embeddings[7])
is_match(embeddings[0], embeddings[8])
is_match(embeddings[0], embeddings[9])