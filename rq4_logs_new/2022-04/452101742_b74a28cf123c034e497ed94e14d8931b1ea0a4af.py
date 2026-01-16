# -*- coding: utf-8 -*-
"""
Created on Sun Feb  6 19:27:20 2022

This is our gui file
"""

import numpy as np
from PIL import Image
from PIL import ImageTk
import tkinter as tk
from tensorflow import keras
from tkinter import filedialog as fd
from tkinter import messagebox

from GAN32 import GAN32Model
from GAN64 import GAN64Model
from GAN128 import GAN128Model
from AutoEncoder import AutoEncoderModel

from gcs_script import download_files_from_bucket, list_files_in_gcs
from os.path import exists
import os

#check model list first
DIR = os.path.dirname(__file__) + '/saved_models/'
for name in list_files_in_gcs():
    if not exists(DIR + name):
        print('downloading model:' + name)
        download_files_from_bucket(name, DIR + name)

#models
ae =  AutoEncoderModel()
encoder = ae.getModel().layers[1]
decoder = ae.getModel().layers[2]
gan32 = GAN32Model().getModel().layers[2]
gan64 = GAN64Model().getModel().layers[2]
gan128 = GAN128Model().getModel().layers[2]

#tkinter components
beforeImageLabel = None
afterImageLabel = None

pathLabel = None
imgLoaded = False
imgArr = None

def selectButtonCallback():
    global imgArr
    global pathLabel
    global imgLoaded
    global beforeImageLabel
    
    fileName = fd.askopenfilename(filetypes = [('jpeg', '.jpeg .jpg'), ('bmp', '.bmp'), ('png', '.png')])
    
    #if user press cancel
    if len(fileName) == 0:
        return
    
    #read image
    try:
        loadedImage = Image.open(fileName).resize((256, 256))
        
        imgArr = np.array(loadedImage)/256
        imgArr = np.expand_dims(imgArr, 0)
        
        #show image in image label
        img = ImageTk.PhotoImage(image = loadedImage.resize((400, 400)))
        beforeImageLabel.configure(image = img)
        beforeImageLabel.image = img
        
        pathLabel.config(text = 'Selected Image: ' + fileName)
        imgLoaded = True
        
    except Exception as e:
        messagebox.showinfo('Unable to load this image', e)
        
def genImg(img, paraVector):
    output = keras.layers.Resizing(32, 32)(img)
    output = gan32.predict([output, paraVector])
    output = gan64.predict([output, paraVector])
    output = gan128.predict([output, paraVector])
    return output
        
def generateButtonCallback():
    global imgArr
    global afterImageLabel
    
    #check if image is loaded
    if not imgLoaded:
        messagebox.showinfo('error', 'Please select an image first')
        return
    
    #generate random feature vector
    paraVector = np.random.normal(0, 1, (1, 400))
    output = genImg(imgArr, paraVector)
    
    #display
    img = Image.fromarray((output[0] * 256).astype(np.uint8))
    image = ImageTk.PhotoImage(image = img.resize((400, 400)))
    afterImageLabel.configure(image = image)
    afterImageLabel.image = image
    

#our main window
window = tk.Tk()
window.title('Fake Image Generator')
window.geometry('1024x768')

#disable maximize button
window.resizable(0, 0)

#add button callbacks
selectButton = tk.Button(window, text = 'Select an image', command = selectButtonCallback)
selectButton.place(x = 10, y = 10)

generateButton = tk.Button(window, text = 'Generate', command = generateButtonCallback)
generateButton.place(x = 110, y = 10)

#add labels
pathLabel = tk.Label(window, text = 'Selected Image: ')
pathLabel.place(x = 10, y = 50)

#two picture labels
beforeImageLabel = tk.Label(window)
afterImageLabel = tk.Label(window)
beforeImageLabel.place(x = 60, y = 200)
afterImageLabel.place(x = 540, y = 200)

#window update loop
window.mainloop()
