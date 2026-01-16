#!/usr/bin/env python

import matplotlib
matplotlib.use('Agg')
import os
os.environ["KERAS_BACKEND"] = "tensorflow"
import argparse
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import datetime
from keras.layers import Input
from keras.models import Model, Sequential
from keras.layers.core import Reshape, Dense, Dropout, Flatten
from keras.layers.advanced_activations import LeakyReLU
from keras.layers.convolutional import Convolution2D, UpSampling2D
from keras.layers import BatchNormalization####
from keras.datasets import mnist
from keras.optimizers import Adam
from keras import backend as K
from keras import initializers
import random
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
import pickle

#K.set_image_dim_ordering('th')

# Deterministic output.
# Tired of seeing the same results every time? Remove the line below.
#np.random.seed(1000)

# The results are a little better when the dimensionality of the random vector is only 10.
# The dimensionality has been left at 100 for consistency with other GAN implementations.
randomDim = 100


#######rows, cols = 4, 14
#input_shape = (4,14)
input_minmax_scaler = 0
#input_dim = cols * rows

def myprint(model,dirtopology,model_name):
    with open(dirtopology+'/'+str(model_name)+'_report.txt','w') as fh:
        # Pass the file handle in as a lambda function to make it callable
        model.summary(print_fn=lambda x: fh.write(x + '\n'))


def sci_minmax(X,input_minmax_scaler=None):
    if input_minmax_scaler is None:
        input_minmax_scaler = MinMaxScaler(feature_range=(-1, 1), copy=True)
    return (input_minmax_scaler.fit_transform(X), input_minmax_scaler)


def import_data(filename=["../data/newmergeofData_025.txt","../data/newmergeofData_01.txt" ]):#folder,namefile):
    #mergeofData  input_training newmergeofData_025.txt newmergeofData_01.txt
    #input_data = np.loadtxt("../data/newmergeofData_025.txt",delimiter=';')
    print("Filename: ", filename[0])
    input_data =np.loadtxt(str(filename[0]), delimiter=';')####
    #for i, f in enumerate(filename, start=1):
    #    input_data_temp = np.loadtxt(f, delimiter=';')
    #    #input_data2 = np.loadtxt("../data/newmergeofData_01.txt",delimiter=';')
    #    input_data=np.concatenate((input_data,input_data_temp),axis=0)

    (input_data, input_minmax_scaler) = sci_minmax(input_data)
    print("input dim ", input_data.shape)
    if (input_data.shape[0]%rows == 0):
        X_train = input_data.reshape( input_data.shape[0]//rows, cols* rows)
        print ("X_train dimension " , X_train.shape)
    else:
        input_data = input_data[:-(input_data.shape[0]%rows)]
        X_train = input_data.reshape( input_data.shape[0]//rows, cols* rows)
        print ("X_train dimension " , X_train.shape)

    return X_train,input_minmax_scaler

# Plot the loss from each batch
def plotLoss(epoch,dirr,batch):
    plt.figure(figsize=(10, 8))
    plt.plot(dLosses, label='Discriminitive loss')
    plt.plot(gLosses, label='Generative loss')
    plt.xlabel('Epoch')
    plt.ylabel('Loss')
    plt.legend()
    plt.savefig(dirr+'/gan_loss_bs_'+str(batch)+'_epoch %d.png' % epoch)
    np.savetxt(dirr+'/bs_'+str(batch)+'_dLoss.csv', np.array(dLosses),fmt="%f",delimiter=',')
    np.savetxt(dirr+'/bs_'+str(batch)+'_gLoss.csv', np.array(gLosses),fmt="%f",delimiter=',')

#Save The generated Movements
def saveGeneratedMovements(epoch, dirr,batch,examples=100):
    noise = np.random.uniform(-1,1,size=[examples, randomDim])
    generated_movements = generator.predict(noise)
    generated_movements = generated_movements.reshape(examples, rows, cols)
    for i,movement in enumerate(generated_movements, start=0):
        np.savetxt(dirr+"/n_batch_"+str(batch)+"_generatedMovements_epoch_"+str(epoch)+"_movement_"+str(i)+".csv", input_minmax_scaler.inverse_transform(movement),fmt="%f",delimiter=',')
    return

# Save the generator and discriminator networks (and weights) for later use
def saveModels(epoch,dirmodels,gen,discr):
    gen.save(dirmodels + '/gan_generator_epoch_%d.h5' % epoch)
    discr.save(dirmodels + '/gan_discriminator_epoch_%d.h5' % epoch)


def loadModels(epoch,dirmodels):
    generatorLoad = load_model(dirmodels + '/gan_generator_epoch_%d.h5' % epoch)
    discriminatorLoad = load_model(dirmodels + '/gan_discriminator_epoch_%d.h5' % epoch)
    return generatorLoad, discriminatorLoad

def train(epochs=1, batchSize=32, unit_of_movement=4, n_example=100, train_file='/home/bee/robotak/rsait-crss/python/gan/generation/data/input_training.txt', mocap='openpose'):
    #Creation of the model

    # Optimizer
    adam = Adam(lr=0.0002, beta_1=0.5)
    global generator
    generator = Sequential()
    generator.add(Dense(128, input_dim=randomDim, kernel_initializer=initializers.RandomNormal(stddev=0.02)))
    generator.add(LeakyReLU(0.2))
    generator.add(Dense(256))
    generator.add(LeakyReLU(0.2))
    generator.add(Dense(512))
    generator.add(LeakyReLU(0.2))
    # generator.add(Dense(1024))
    # generator.add(LeakyReLU(0.2))
    generator.add(Dense(input_dim, activation='tanh'))
    generator.compile(loss='binary_crossentropy', optimizer=adam)

    discriminator = Sequential()
    # discriminator.add(Dense(1024, input_dim=input_dim, kernel_initializer=initializers.RandomNormal(stddev=0.02)))
    # discriminator.add(LeakyReLU(0.2))
    # discriminator.add(Dropout(0.3))
    # discriminator.add(Dense(512))
    discriminator.add(Dense(512, input_dim=input_dim, kernel_initializer=initializers.RandomNormal(stddev=0.02)))
    discriminator.add(LeakyReLU(0.2))
    discriminator.add(Dropout(0.3))
    discriminator.add(Dense(256))
    discriminator.add(LeakyReLU(0.2))
    discriminator.add(Dropout(0.3))
    discriminator.add(Dense(128))
    discriminator.add(LeakyReLU(0.2))
    discriminator.add(Dropout(0.3))
    discriminator.add(Dense(1, activation='sigmoid'))
    discriminator.compile(loss='binary_crossentropy', optimizer='adam') #=adam)

    # Combined network
    discriminator.trainable = False
    ganInput = Input(shape=(randomDim,))
    x = generator(ganInput)
    ganOutput = discriminator(x)
    gan = Model(inputs=ganInput, outputs=ganOutput)
    gan.compile(loss='binary_crossentropy', optimizer=adam)

    global dLosses,gLosses
    dLosses = []
    gLosses = []
    global input_minmax_scaler
    X_train,input_minmax_scaler = import_data([train_file])
    batchCount = X_train.shape[0] / batchSize
    print ('Epochs:', epochs)
    print ('Batch size:', batchSize)
    print ('Batches per epoch:', batchCount)

    for e in range(1, epochs+1):
        print ('-'*15, 'Epoch %d' % e, '-'*15)
        for _ in tqdm(range(int(batchCount))):

            noise = np.random.uniform(-1,1, size=[batchSize, randomDim])
            imageBatch = X_train[np.random.randint(0, X_train.shape[0], size=batchSize)]

            # Generate fake MNIST images
            generatedImages = generator.predict(noise)
            # print np.shape(imageBatch), np.shape(generatedImages)
            X = np.concatenate([imageBatch, generatedImages])

            # Labels for generated and real data
            yDis = np.zeros(2*batchSize)
            # One-sided label smoothing
            yDis[:batchSize] = 0.9

            # Train discriminator
            discriminator.trainable = True
            dloss = discriminator.train_on_batch(X, yDis)

            # Train generator
            noise = np.random.uniform(-1,1,size=[batchSize, randomDim])
            yGen = np.ones(batchSize)
            discriminator.trainable = False
            gloss = gan.train_on_batch(noise, yGen)

        print('Discriminator Loss ={}, GAN loss ={}'.format(dloss, gloss))

        # Store loss of most recent batch from this epoch
        dLosses.append(dloss)
        gLosses.append(gloss)

        if e == 1:
            if(e==1):
                now = datetime.datetime.now()
                #timestamp=now.strftime("%Y-%m-%d-%H:%M")
                timestamp=now.strftime("%Y-%m-%d")
                dirr = '../samples/' + mocap + '/' + timestamp + '/' + mocap + '_n_epochs_' + str(epochs) + "_batchSize_" + str(batchSize) + "_um_" + str(unit_of_movement)# + '_' + str(timestamp)
                dirmodels = '../models/' + mocap + '/' + timestamp + '/' + mocap + '_n_epochs_' + str(epochs) + "_batchSize_" + str(batchSize) + "_um_" + str(unit_of_movement)# + '_' + str(timestamp)
                dirrloss = dirmodels + '/loss_file/'
                dirtopology = dirmodels + '/topology_net/'

        if(e == epochs):
            if not os.path.exists(dirr):
                os.makedirs(dirr)
            if not os.path.exists(dirrloss):
                os.makedirs(dirrloss)
            if not os.path.exists(dirmodels):
                os.makedirs(dirmodels)
            if not os.path.exists(dirtopology):
                os.makedirs(dirtopology)
            saveGeneratedMovements(e,dirr,batchSize,n_example)

    #save in a file the models
    myprint(discriminator,dirtopology,'discriminator')
    myprint(generator,dirtopology,'generator')
    myprint(gan,dirtopology,'gan')


    plotLoss(e,dirrloss,batchSize) # Plot losses from every epoch
    saveModels(e, dirmodels,generator, discriminator)     #save a model
    filehandler = open(dirmodels+'/pickle_min_maxScaler', 'wb')  #save a pickle for min_max_scaler
    pickle.dump(input_minmax_scaler, filehandler, protocol=2)

if __name__ == '__main__':
    global rows, cols, input_dim
    parser = argparse.ArgumentParser()
    parser.add_argument("--e",type=int, default=10, help="Number of epochs for training.")
    parser.add_argument("--bs", type=int, default=32, help="Batch Size ")
    parser.add_argument("--um", type=int, default=4, help="Number of poses that composes a Unit of Movement")
    parser.add_argument("--ne", type=int, default=100, help="Number of example generated for each epochs")
    parser.add_argument("--db", type=str, default='/home/bee/robotak/rsait-crss/python/gan/generation/data/input_training.txt', help="Path of the training database")
    parser.add_argument("--mocap", type=str, default='openpose', help="Used motion capturing system")
    args = parser.parse_args()
    rows, cols = args.um, 60
    input_dim = cols * rows
    train(args.e, args.bs, args.um, args.ne, args.db, args.mocap)