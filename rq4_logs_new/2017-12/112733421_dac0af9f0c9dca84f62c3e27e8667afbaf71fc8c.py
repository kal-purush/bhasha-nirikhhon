# -*- coding: utf-8 -*-
"""
Created on Fri Dec  1 12:50:19 2017

@author: Stephane
code inspired by Deeplearning.ai - Coursera course from Andrew Ng
https://www.coursera.org/learn/convolutional-neural-networks

"""

import os
import sys
import importlib
import scipy.io
import scipy.misc
import matplotlib.pyplot as plt
from matplotlib.pyplot import imshow
from PIL import Image
import numpy as np
import tensorflow as tf
import time


import nst_utils, nst_functions
importlib.reload(nst_utils)
importlib.reload(nst_functions)
from nst_utils import *
from nst_functions import *

# set your working directory
os.chdir(CONFIG.WORKING_DIR)

#load content and style image from ./images directory
(content_image, style_image, generated_image) = get_images()

# Reset the graph
tf.reset_default_graph()
# Start interactive session
sess = tf.InteractiveSession()
# load vgg19 model from disk in ./pretrained-model directory
model = load_vgg_model("pretrained-model/imagenet-vgg-verydeep-19.mat")

# Assign the content image to be the input of the VGG model. 
sess.run(model['input'].assign(content_image))
   
# Select the output tensor of layer conv4_2
out = model['conv4_2']

# Set a_C to be the hidden layer activation from the layer we have selected
a_C = sess.run(out)

# Set a_G to be the hidden layer activation from same layer. Here, a_G references model['conv4_2'] 
# and isn't evaluated yet. Later in the code, we'll assign the image G as the model input, so that
# when we run the session, this will be the activations drawn from the appropriate layer, with G as input.
a_G = out

# Compute the content cost
J_content = compute_content_cost(a_C, a_G)

# Assign the input of the model to be the "style" image 
sess.run(model['input'].assign(style_image))

# Compute the style cost
J_style = compute_style_cost(sess, model, CONFIG.STYLE_LAYERS)

# compute the total cost
J = total_cost(J_content, J_style, alpha = CONFIG.ALPHA, beta = CONFIG.BETA)

# optimize and train, produce output images to be saved in ./output diretory
start_time = time.time()
model_nn(sess, model, J, J_content, J_style, generated_image, num_iterations = CONFIG.NBR_ITERATIONS)

#measure training time
elapsed_time = time.time() - start_time
print("training time =  ", elapsed_time)

# close the session
sess.close()