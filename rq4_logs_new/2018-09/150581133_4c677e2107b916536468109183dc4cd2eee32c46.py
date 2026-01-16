
# coding: utf-8

# In[9]:


import math
import numpy as np
import matplotlib.pyplot as plt

from keras import optimizers, regularizers, applications
from keras.models import Sequential, Model
from keras.layers import Dense, Flatten, Dropout, GlobalAveragePooling2D
from keras.preprocessing.image import ImageDataGenerator

plt.style.use('ggplot')
plt.switch_backend('agg')


# In[10]:


import tensorflow as tf 


# In[11]:


EPOCHS = 60
N_VAL = 1477
N_TRAIN = 3423
BATCH_SIZE = 128
RESOLUTION = 224
EXP_NAME = 'VGG16_e001'
PATH_TRAIN = './data/faces/sample/train'
PATH_VAL = './data/faces/sample/valid'


# In[12]:


def overfitting_plot(history, name):
    acc = history.history['acc']
    val_acc = history.history['val_acc']
    loss = history.history['loss']
    val_loss = history.history['val_loss']
    
    epochs = range(1, len(acc) + 1)
    
    plt.plot(epochs, acc, 'bo', label='Training acc')
    plt.plot(epochs, val_acc, 'b', label='Validation acc')
    plt.title('Training and validation accuracy')
    plt.legend(loc='best')
    plt.savefig('./data/faces/plots/acc/'+'acc_'+name+'.png')
    
    plt.figure()
    plt.plot(epochs, loss, 'bo', label='Training loss')
    plt.plot(epochs, val_loss, 'b', label='Validation loss')
    plt.title('Training and validation loss')
    plt.legend(loc='best')
    plt.savefig('./data/faces/plots/loss/'+'loss_'+name+'.png')


# In[13]:


def train(model, path_train, path_val, name=' '):
    
    train_datagen = ImageDataGenerator(rescale=1./255)
    val_datagen = ImageDataGenerator(rescale=1./255)
    
    train_generator = train_datagen.flow_from_directory(
        path_train,
        target_size=(RESOLUTION, RESOLUTION),
        batch_size=BATCH_SIZE,
        class_mode='categorical')
    
    val_generator = val_datagen.flow_from_directory(
        path_val,
        target_size=(RESOLUTION, RESOLUTION),
        batch_size=BATCH_SIZE,
        class_mode='categorical')
    
    history = model.fit_generator(
        train_generator,
        steps_per_epoch=(N_TRAIN // BATCH_SIZE),
        epochs=EPOCHS,
        validation_data=val_generator ,
        validation_steps=(N_VAL // BATCH_SIZE)
    )
    
    model.save('./data/faces/sample/models/'+name+'.h5')
    
    overfitting_plot(history, name)


# In[14]:


def model_base(freeze_conv=True, name=' '):
    if(name == 'VGG16'):
        model = applications.VGG16(include_top=False,
                                   input_shape=(RESOLUTION,RESOLUTION,3), 
                                   weights='imagenet')
    elif(name == 'InceptionV3'):
        model = applications.InceptionV3(include_top=False,
                                         input_shape=(RESOLUTION,RESOLUTION,3), 
                                         weights='imagenet')
    if(freeze_conv):
        model.trainable = False
    else:
        model.trainable = True
    return model


# In[15]:


def vgg16_pretrained_model(with_dropout=False, with_regularizer=False, regularizer_weight=0.001):
    
    model_vgg16 = model_base(name='VGG16')
    model = Sequential()
    model.add(model_vgg16)
    model.add(Flatten())
    
    if with_dropout:
        model.add(Dropout(0.5))
    if with_regularizer:
        model.add(Dense(256, activation='relu',
                        kernel_regularizer=regularizers.l1_l2(l1=regularizer_weight, l2=regularizer_weight)))
    else:
        model.add(Dense(256, activation='relu'))
    
    model.add(Dense(7, activation=tf.nn.softmax))
    
    model.compile(loss='categorical_crossentropy', 
                  optimizer=optimizers.RMSprop(lr=1e-5), 
                  metrics=['acc'])
    return model


# In[16]:


model = vgg16_pretrained_model()


# In[ ]:


train(model, PATH_TRAIN, PATH_VAL, name=EXP_NAME)
