# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 22:44:32 2022
"""

from Wrapper import ModelWrapper
import numpy as np
from utils.ImageLoader import ImageLoader
from tensorflow import keras
from matplotlib import pyplot as plt
import pickle

'''
Generator part for GAN128 model
'''
class Generator128(ModelWrapper):
    
    def __block(self, size : int, in_depth : int, out_depth : int, kSize):
        
        inX = keras.layers.Input(shape = (size, size, in_depth))
        Y = keras.layers.Conv2D(out_depth, kernel_size = kSize, padding = 'same')(inX)
        Y = keras.layers.BatchNormalization(momentum = 0.8)(Y)
        Y = keras.layers.ReLU()(Y)
        return keras.Model(inX, Y)
    
    def __blockInverse(self, size : int, in_depth : int, out_depth : int, kSize):
        inX = keras.layers.Input(shape = (size, size, in_depth))
        Y = keras.layers.UpSampling2D(size = (2, 2))(inX)
        Y = keras.layers.Conv2D(out_depth, kernel_size = 1)(Y)
        Y = keras.layers.BatchNormalization(momentum = 0.8)(Y)
        Y = keras.layers.ReLU()(Y)
        
        return keras.Model(inX, Y)
    
    def _define_model(self):
        
        # input 128 * 128 image-----------------------
        X0 = keras.layers.Input(shape = (128, 128, 3))
        Y0 = self.__block(128, 3, 400, 4)(X0)
        #----------------------------------------------
        
        #parameter vector-----------------------------
        X1 = keras.layers.Input(shape = (400,))
        Y1 = keras.layers.Dense(4096)(X1)
        Y1 = keras.layers.Reshape((2, 2, 1024))(Y1)
        Y1 = keras.layers.BatchNormalization(momentum = 0.8)(Y1)
        Y1 = keras.layers.ReLU()(Y1)
        #---------------------------------------------
        
        #up-scale to 4 * 4 * 800
        Y1 = self.__blockInverse(2, 1024, 800, 2)(Y1)
        Y1 = self.__block(4, 800, 800, 2)(Y1)
        
        #up-scale to 8 * 8 * 600
        Y1 = self.__blockInverse(4, 800, 600, 2)(Y1)
        Y1 = self.__block(8, 600, 600, 2)(Y1)
        
        #up-scale to 16 * 16 * 500
        Y1 = self.__blockInverse(8, 600, 500, 2)(Y1)
        Y1 = self.__block(16, 500, 500, 2)(Y1)
        
        #up-scale to 32 * 32 * 460
        Y1 = self.__blockInverse(16, 500, 460, 3)(Y1)
        Y1 = self.__block(32, 460, 460, 3)(Y1)
        
        #up-scale to 64 * 64 * 430
        Y1 = self.__blockInverse(32, 460, 430, 3)(Y1)
        Y1 = self.__block(64, 430, 430, 3)(Y1)
        
        #up-scale to 64 * 64 * 400
        Y1 = self.__blockInverse(64, 430, 400, 4)(Y1)
        Y1 = self.__block(128, 400, 400, 4)(Y1)
        
        #concat Y0 and Y1, output 128 * 128 * 800
        Y2 = keras.layers.Concatenate()([Y0, Y1])
        
        #up-scale to 256 * 256 * 512
        Y2 = self.__blockInverse(128, 800, 800, 4)(Y2)
        Y2 = self.__block(256, 800, 800, 2)(Y2)
        Y2 = self.__block(256, 800, 512, 5)(Y2)
        
        #output 256 * 256 * 3
        Y2 = keras.layers.Conv2D(3, (4, 4), padding = 'same')(Y2)
        Y2 = keras.layers.Activation('sigmoid')(Y2)
        
        return keras.Model([X0, X1], Y2), 'Generator128'
        
        
    def _compile_model(self, model):
        pass

'''
Discriminator part for GAN128 model
'''
class Discriminator128(ModelWrapper):
    
    def __block(self, size : int, in_depth : int, out_depth : int, kSize):
        
        inX = keras.layers.Input(shape = (size, size, in_depth))
        Y = keras.layers.Conv2D(out_depth, kernel_size = kSize, padding = 'same')(inX)
        Y = keras.layers.LeakyReLU(alpha = 0.2)(Y)
        
        return keras.Model(inX, Y)
    
    def _define_model(self):
        
        #take 256 * 256 * 3 image
        X0 = keras.layers.Input(shape = (256, 256, 3))
        
        vgg16 = keras.applications.VGG16(include_top = False, weights = "imagenet")

        #set only last 4 layers trainable (fine tune)
        for i in range(len(vgg16.layers)):
            if i < len(vgg16.layers) - 9:
                vgg16.layers[i].trainable = False
            else:
                break
        
        #resize to 224 * 224
        Y0 = keras.layers.Resizing(224, 224)(X0)
        
        #go through vgg16
        Y0 = vgg16(Y0)
        
        # output 2
        Y0 = keras.layers.Flatten()(Y0)
        Y0 = keras.layers.Dense(2)(Y0)
        Y0 = keras.layers.Activation('softmax')(Y0)
        
        return keras.Model(X0, Y0), 'Discriminator128'
        
    def _compile_model(self, model):
        model.compile(optimizer = keras.optimizers.RMSprop(learning_rate = 0.00001), loss = keras.losses.CategoricalCrossentropy())

'''
GAN model that output 256x256 image
'''
class GAN128Model(ModelWrapper):
    
    __generator = None
    __generatorWrapper = None
    __discriminator = None
    __discriminatorWrapper = None
    
    def __init__(self):
        
        #load generator and discriminator model first
        self.__generatorWrapper = Generator128()
        self.__discriminatorWrapper = Discriminator128()
        
        self.__generator = self.__generatorWrapper.getModel()
        self.__discriminator = self.__discriminatorWrapper.getModel()
        print('Please ignore "unable to load GAN32 model, GAN model load generator and discriminator separately."')
        super().__init__()
    
    def _define_model(self):
        #construct combined model, this combined model only train the generator, so set discriminator untrainable
        #two inputs for generator one for feature vector and one for parameter vector
        X0 = keras.layers.Input(shape = (128, 128, 3))
        X1 = keras.layers.Input(shape = (400,))
        
        #pass X0, X1 to generator
        Y0 = self.__generator([X0, X1])
        
        #set discriminator untrainable
        self.__discriminator.trainable = False
        
        #connect the generator to the discriminator
        Y0 = self.__discriminator(Y0)
        
        return keras.Model([X0, X1], Y0), 'GAN128'
        
        
    def _compile_model(self, model):
        model.compile(optimizer = keras.optimizers.RMSprop(learning_rate = 0.00001), loss = keras.losses.CategoricalCrossentropy())
        
    def save(self):
        self.__generatorWrapper.save()
        
        #we only need generator
        #self.__discriminatorWrapper.save()

    def train(self, x, steps_per_epoch, batch_size, epochs, autoSave = 5, **kwargs):
        
        #losses for generator and discriminator
        disLoss = 0
        genLoss = 0
        
        autoSaveCounter = 0
        
        #true and fake labels
        trueLabel = np.ones((batch_size, 2))
        falseLabel = np.ones((batch_size, 2))
        
        trueLabel[:, 1] = 0
        falseLabel[:, 0] = 0
        
        #simple model to shrink image
        def shrinkModel():
            x = keras.layers.Input(shape = (256, 256, 3))
            y = keras.layers.Resizing(128, 128)(x)
            return keras.Model(x, y)
        
        
        shrink = shrinkModel()
        
        for ep in range(epochs):
            
            
            for step in range(steps_per_epoch):
                
                #parameters for generator
                parameterVector = np.random.normal(0, 1, (batch_size, 400))
                upScaleImg = shrink.predict(next(x))
                
                #generate images
                fakeX = self.__generator.predict(x = [upScaleImg ,parameterVector])
                realX = next(x)
                
                
                #train discriminator
                disLoss = 0.5 * self.__discriminator.train_on_batch(x = fakeX, y = falseLabel)
                disLoss += 0.5 * self.__discriminator.train_on_batch(x = realX, y = trueLabel)
                
                #train generator
                genLoss = self._model.train_on_batch(x = [upScaleImg, parameterVector], y = trueLabel)
                
                print('epoch', ep, 'generator loss:', genLoss, 'discriminator loss:', disLoss)
                 
            #autosave
            autoSaveCounter+=1
            
            if autoSaveCounter >= autoSave:
                self.save()
                print('generator and discriminator auto saved')
                autoSaveCounter = 0
        

def gan128_train(batchSize : int, epoch : int):
    gan = GAN128Model()
    imgLoader = ImageLoader('../data/dataset/')
    
    imageGenerator = imgLoader.getGenerator(batchSize, imageSize = 256)
    
    gan.train(x = imageGenerator, steps_per_epoch = 20, batch_size = batchSize, epochs = epoch)
    gan.save()
