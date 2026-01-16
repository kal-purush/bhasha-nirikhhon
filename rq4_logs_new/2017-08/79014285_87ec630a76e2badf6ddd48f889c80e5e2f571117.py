"""

Objective
=========
Trains a Resnet50 network on CIFAR10 small image dataset.

Benchmarks
==========

MXNet Backend
=============

Takes lot of time on a CPU machine. Recommended to run on GPU machine.

Increase number of epochs up to 200 for accuracy > 0.94

Epochs - 10
Batchsize - 32 per GPU.

Example
1 GPU   -> Batchsize = 32
2 GPU   -> Batchsize = 64
4 GPU   -> Batchsize = 128
8 GPU   -> Batchsize = 256
16 GPU  -> Batchsize = 512

1 GPU
=====
        Average time per epoch: 160.93
        Train Accuracy: 0.69
        Test Accuracy: 0.67

2 GPU
=====
        Average time per epoch: 97.19
        Train Accuracy: 0.69
        Test Accuracy: 0.52

4 GPU
=====
        Average time per epoch: 51.49
        Train Accuracy: 0.69
        Test Accuracy: 0.60

8 GPU
=====
        Average time per epoch: 41.80
        Train Accuracy: 0.68
        Test Accuracy: 0.59

16 GPU
=====
        Average time per epoch: 41.36
        Train Accuracy: 0.54
        Test Accuracy: 0.42

You can edit the following parameters for your experimentation:
1. batch_size - Batch size.
2. nb_epoch - Number of epochs.

References/Credits
==================
Resnet50 code is borrowed from - https://github.com/raghakot/keras-resnet/blob/master/resnet.py

"""

from __future__ import print_function
from __future__ import division

import numpy as np

import six

from keras.datasets import cifar10
from keras.preprocessing.image import ImageDataGenerator
from keras.optimizers import SGD
from keras.utils import np_utils

from keras.models import Model
from keras.layers import (
    Input,
    Activation,
    merge,
    Dense,
    Flatten
)
from keras.layers.convolutional import (
    Convolution2D,
    MaxPooling2D,
    AveragePooling2D
)
from keras.layers.normalization import BatchNormalization
from keras.regularizers import l2
from keras import backend as K

np.random.seed(1337)  # for reproducibility


def _bn_relu(input):
    """Helper to build a BN -> relu block
    """
    norm = BatchNormalization(axis=CHANNEL_AXIS)(input)
    return Activation("relu")(norm)


def _conv_bn_relu(**conv_params):
    """Helper to build a conv -> BN -> relu block
    """
    nb_filter = conv_params["nb_filter"]
    nb_row = conv_params["nb_row"]
    nb_col = conv_params["nb_col"]
    subsample = conv_params.setdefault("subsample", (1, 1))
    init = conv_params.setdefault("init", "he_normal")
    border_mode = conv_params.setdefault("border_mode", "same")
    W_regularizer = conv_params.setdefault("W_regularizer", l2(1.e-4))

    def f(input):
        conv = Convolution2D(nb_filter=nb_filter, nb_row=nb_row, nb_col=nb_col, subsample=subsample,
                             init=init, border_mode=border_mode, W_regularizer=W_regularizer)(input)
        return _bn_relu(conv)

    return f


def _bn_relu_conv(**conv_params):
    """Helper to build a BN -> relu -> conv block.
    This is an improved scheme proposed in http://arxiv.org/pdf/1603.05027v2.pdf
    """
    nb_filter = conv_params["nb_filter"]
    nb_row = conv_params["nb_row"]
    nb_col = conv_params["nb_col"]
    subsample = conv_params.setdefault("subsample", (1,1))
    init = conv_params.setdefault("init", "he_normal")
    border_mode = conv_params.setdefault("border_mode", "same")
    W_regularizer = conv_params.setdefault("W_regularizer", l2(1.e-4))

    def f(input):
        activation = _bn_relu(input)
        return Convolution2D(nb_filter=nb_filter, nb_row=nb_row, nb_col=nb_col, subsample=subsample,
                             init=init, border_mode=border_mode, W_regularizer=W_regularizer)(activation)

    return f


def _shortcut(input, residual):
    """Adds a shortcut between input and residual block and merges them with "sum"
    """
    # Expand channels of shortcut to match residual.
    # Stride appropriately to match residual (width, height)
    # Should be int if network architecture is correctly configured.
    input_shape = K.int_shape(input)
    residual_shape = K.int_shape(residual)
    stride_width = int(round(input_shape[ROW_AXIS] / residual_shape[ROW_AXIS]))
    stride_height = int(round(input_shape[COL_AXIS] / residual_shape[COL_AXIS]))
    equal_channels = input_shape[CHANNEL_AXIS] == residual_shape[CHANNEL_AXIS]

    shortcut = input
    # 1 X 1 conv if shape is different. Else identity.
    if stride_width > 1 or stride_height > 1 or not equal_channels:
        shortcut = Convolution2D(nb_filter=residual_shape[CHANNEL_AXIS],
                                 nb_row=1, nb_col=1,
                                 subsample=(stride_width, stride_height),
                                 init="he_normal", border_mode="valid",
                                 W_regularizer=l2(0.0001))(input)

    return merge([shortcut, residual], mode="sum")


def _residual_block(block_function, nb_filter, repetitions, is_first_layer=False):
    """Builds a residual block with repeating bottleneck blocks.
    """
    def f(input):
        for i in range(repetitions):
            init_subsample = (1, 1)
            if i == 0 and not is_first_layer:
                init_subsample = (2, 2)
            input = block_function(nb_filter=nb_filter, init_subsample=init_subsample,
                                   is_first_block_of_first_layer=(is_first_layer and i == 0))(input)
        return input

    return f


def basic_block(nb_filter, init_subsample=(1, 1), is_first_block_of_first_layer=False):
    """Basic 3 X 3 convolution blocks for use on resnets with layers <= 34.
    Follows improved proposed scheme in http://arxiv.org/pdf/1603.05027v2.pdf
    """
    def f(input):

        if is_first_block_of_first_layer:
            # don't repeat bn->relu since we just did bn->relu->maxpool
            conv1 = Convolution2D(nb_filter=nb_filter,
                                 nb_row=3, nb_col=3,
                                 subsample=init_subsample,
                                 init="he_normal", border_mode="same",
                                 W_regularizer=l2(0.0001))(input)
        else:
            conv1 = _bn_relu_conv(nb_filter=nb_filter, nb_row=3, nb_col=3,
                                  subsample=init_subsample)(input)

        residual = _bn_relu_conv(nb_filter=nb_filter, nb_row=3, nb_col=3)(conv1)
        return _shortcut(input, residual)

    return f


def bottleneck(nb_filter, init_subsample=(1, 1), is_first_block_of_first_layer=False):
    """Bottleneck architecture for > 34 layer resnet.
    Follows improved proposed scheme in http://arxiv.org/pdf/1603.05027v2.pdf
    Returns:
        A final conv layer of nb_filter * 4
    """
    def f(input):

        if is_first_block_of_first_layer:
            # don't repeat bn->relu since we just did bn->relu->maxpool
            conv_1_1 = Convolution2D(nb_filter=nb_filter,
                                 nb_row=1, nb_col=1,
                                 subsample=init_subsample,
                                 init="he_normal", border_mode="same",
                                 W_regularizer=l2(0.0001))(input)
        else:
            conv_1_1 = _bn_relu_conv(nb_filter=nb_filter, nb_row=1, nb_col=1,
                                     subsample=init_subsample)(input)

        conv_3_3 = _bn_relu_conv(nb_filter=nb_filter, nb_row=3, nb_col=3)(conv_1_1)
        residual = _bn_relu_conv(nb_filter=nb_filter * 4, nb_row=1, nb_col=1)(conv_3_3)
        return _shortcut(input, residual)

    return f


def _handle_dim_ordering():
    global ROW_AXIS
    global COL_AXIS
    global CHANNEL_AXIS
    if K.image_dim_ordering() == 'tf':
        ROW_AXIS = 1
        COL_AXIS = 2
        CHANNEL_AXIS = 3
    else:
        CHANNEL_AXIS = 1
        ROW_AXIS = 2
        COL_AXIS = 3


def _get_block(identifier):
    if isinstance(identifier, six.string_types):
        res = globals().get(identifier)
        if not res:
            raise ValueError('Invalid {}'.format(identifier))
        return res
    return identifier


class ResnetBuilder(object):
    @staticmethod
    def build(input_shape, num_outputs, block_fn, repetitions):
        """Builds a custom ResNet like architecture.
        Args:
            input_shape: The input shape in the form (nb_channels, nb_rows, nb_cols)
            num_outputs: The number of outputs at final softmax layer
            block_fn: The block function to use. This is either `basic_block` or `bottleneck`.
                The original paper used basic_block for layers < 50
            repetitions: Number of repetitions of various block units.
                At each block unit, the number of filters are doubled and the input size is halved
        Returns:
            The keras `Model`.
        """
        _handle_dim_ordering()
        if len(input_shape) != 3:
            raise Exception("Input shape should be a tuple (nb_channels, nb_rows, nb_cols)")

        # Permute dimension order if necessary
        if K.image_dim_ordering() == 'tf':
            input_shape = (input_shape[1], input_shape[2], input_shape[0])

        # Load function from str if needed.
        block_fn = _get_block(block_fn)

        input = Input(shape=input_shape)
        conv1 = _conv_bn_relu(nb_filter=64, nb_row=7, nb_col=7, subsample=(2, 2))(input)
        pool1 = MaxPooling2D(pool_size=(3, 3), strides=(2, 2), border_mode="valid")(conv1)

        block = pool1
        nb_filter = 64
        for i, r in enumerate(repetitions):
            block = _residual_block(block_fn, nb_filter=nb_filter, repetitions=r, is_first_layer=(i == 0))(block)
            nb_filter *= 2

        # Last activation
        block = _bn_relu(block)

        # Classifier block
        block_shape = K.int_shape(block)
        pool2 = AveragePooling2D(pool_size=(block_shape[ROW_AXIS], block_shape[COL_AXIS]),
                                 strides=(1, 1))(block)
        flatten1 = Flatten()(pool2)
        dense = Dense(output_dim=num_outputs, init="he_normal", activation="softmax")(flatten1)

        model = Model(input=input, output=dense)
        return model


    @staticmethod
    def build_resnet_50(input_shape, num_outputs):
        return ResnetBuilder.build(input_shape, num_outputs, bottleneck, [3, 4, 6, 3])

    @staticmethod
    def build_resnet_152(input_shape, num_outputs):
        return ResnetBuilder.build(input_shape, num_outputs, bottleneck, [3, 8, 36, 3])

# Step 1. Prepare the training parameters
batch_size = 64
nb_classes = 10
nb_epoch = 10 # Should be atleast 200 for ideal accuracy
data_augmentation = True

# input image dimensions
img_rows, img_cols = 32, 32

# The CIFAR10 images are RGB.
img_channels = 3

# the data, shuffled and split between train and test sets
(X_train, y_train), (X_test, y_test) = cifar10.load_data()

print('X_train shape:', X_train.shape)
print(X_train.shape[0], 'train samples')
print(X_test.shape[0], 'test samples')

# Convert class vectors to binary class matrices.
Y_train = np_utils.to_categorical(y_train, nb_classes)
Y_test = np_utils.to_categorical(y_test, nb_classes)

# Step 2: Build the Resnet50 model for CIFAR 10 image dataset. Images in CIFAR small image dataset is 32*32 RGB (*3).
# There are 10 classes of images
model = ResnetBuilder.build_resnet_50((3, 32, 32), nb_classes)
model.summary()

# Step 3: Compile the model
"""
    For MXNet backend: If you are running on a GPU machine. Assuming you want to use 4 GPUs. Use below code to compile
    the model.

    model.compile(loss='categorical_crossentropy', optimizer=SGD(), metrics=['accuracy'], context=["gpu(0)", "gpu(1)",
                                                                                                  "gpu(2)", "gpu(3)"])
"""
model.compile(loss='categorical_crossentropy', optimizer=SGD(), metrics=['accuracy'])

X_train = X_train.astype('float32')
X_test = X_test.astype('float32')
X_train /= 255
X_test /= 255

# Step 4: Train the model.
if not data_augmentation:
    print('Not using data augmentation.')
    history = model.fit(X_train, Y_train,
                        batch_size=batch_size,
                        nb_epoch=nb_epoch,
                        validation_data=(X_test, Y_test),
                        shuffle=True)
    print("Model training complete.")
    print("TRAINING ACCURACY - ", history.history['acc'][-1])
else:
    print('Using real-time data augmentation.')
    # This will do preprocessing and realtime data augmentation:
    datagen = ImageDataGenerator(
        featurewise_center=False,  # set input mean to 0 over the dataset
        samplewise_center=False,  # set each sample mean to 0
        featurewise_std_normalization=False,  # divide inputs by std of the dataset
        samplewise_std_normalization=False,  # divide each input by its std
        zca_whitening=False,  # apply ZCA whitening
        rotation_range=0,  # randomly rotate images in the range (degrees, 0 to 180)
        width_shift_range=0.1,  # randomly shift images horizontally (fraction of total width)
        height_shift_range=0.1,  # randomly shift images vertically (fraction of total height)
        horizontal_flip=True,  # randomly flip images
        vertical_flip=False)  # randomly flip images

    # Compute quantities required for featurewise normalization
    # (std, mean, and principal components if ZCA whitening is applied).
    datagen.fit(X_train)

    # Fit the model on the batches generated by datagen.flow().
    history = model.fit_generator(datagen.flow(X_train, Y_train,
                                               batch_size=batch_size),
                                  samples_per_epoch=X_train.shape[0],
                                  nb_epoch=nb_epoch,
                                  validation_data=(X_test, Y_test))
    print("Model training complete.")
    print("TRAINING ACCURACY - ", history.history['acc'][-1])

# STEP 5: Compute Test Accuracy.
score = model.evaluate(X_test, Y_test, verbose=0)
print("TEST ACCURACY - ", score[1])