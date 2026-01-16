from keras.applications import ResNet50
from keras.models import Model
import keras.backend as K
from keras.layers import (Conv2D, LeakyReLU, Input, BatchNormalization, Add,
                          MaxPooling2D, Lambda, concatenate, Conv3D, Dropout,
                          Activation, Flatten, Dense)
from keras.regularizers import l1_l2

from .neural_layers import residual_block, bilinear_resize, expand_dims

def create_network(input_shape, prior, L1=0, L2=0, dropout=0, KERNEL_NUM=64):
    resnet = ResNet50(include_top=False, input_shape=input_shape)
    feature_model = Model(resnet.input, resnet.layers[80].output)
    for l in feature_model.layers:
        l.trainable = False

    inp_pos = Input(shape=(3,))
    inp_img = Input(shape=input_shape)
    features = feature_model(inp_img)

    # Downsampling
    x = Conv2D(KERNEL_NUM, 3, padding='same', strides=2, kernel_regularizer=l1_l2(l1=L1, l2=L2))(features)
    x = BatchNormalization()(x)
    x = LeakyReLU()(x)

    # Trainable residual blocks
    x = residual_block(x, bottleneck_kernels=KERNEL_NUM//4,
                               out_kernels=KERNEL_NUM,
                               kernel_size=3,
                               identity=False,
                               L1=L1,
                               L2=L2)

    for i in range(1): # Originally 5
        x = residual_block(x, bottleneck_kernels=KERNEL_NUM//4,
                                    out_kernels=KERNEL_NUM,
                                    kernel_size=3,
                                    identity=True,
                                    L1=L1,
                                    L2=L2)

    x = Conv2D(KERNEL_NUM//2, 1, padding='same', kernel_regularizer=l1_l2(l1=L1, l2=L2))(x)
    x = BatchNormalization()(x)
    x = LeakyReLU()(x)

    x = Lambda(f, name='add_pos')([x, inp_pos])

    x = Flatten()(x)
    x = Dense(32, activation='relu',kernel_regularizer=l1_l2(l1=L1, l2=L2))(x)
    x = Dropout(dropout)(x)
    x = Dense(32, activation='relu',kernel_regularizer=l1_l2(l1=L1, l2=L2))(x)
    x = Dropout(dropout)(x)
    x = Dense(1160, activation='sigmoid')(x)
    return Model(inputs=(inp_img,inp_pos), outputs=x)

def f(compound):
    x, inp_pos = compound
    a = K.ones_like(K.expand_dims(x[:,:,:,0]))
    a = a*K.expand_dims(K.expand_dims(K.expand_dims(inp_pos[:,0])))

    b = K.ones_like(K.expand_dims(x[:,:,:,0]))
    b = b*K.expand_dims(K.expand_dims(K.expand_dims(inp_pos[:,1])))

    c = K.ones_like(K.expand_dims(x[:,:,:,0]))
    c = c*K.expand_dims(K.expand_dims(K.expand_dims(inp_pos[:,2])))
    x = K.concatenate([x,a,b,c], axis=-1)
    return x