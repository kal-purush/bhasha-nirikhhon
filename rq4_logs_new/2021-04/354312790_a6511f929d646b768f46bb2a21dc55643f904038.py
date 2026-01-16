import tensorflow as tf
import tensorflow.keras as k
from tensorflow.keras.layers import Conv2D, MaxPooling2D, BatchNormalization, GlobalAvgPool2D, Flatten, Dense

from utils.layers import Conv2D_BN

"""
 For Resnet34
"""
class ResConvBlockA(k.layers.Layer):
  def __init__(self, filters, strides=(2,2)):
    super(ResConvBlockA, self).__init__()
    self.conv_1a = Conv2D_BN(filters=filters, kernel_size=3, strides=strides)
    self.conv_1b = Conv2D_BN(filters=filters, kernel_size=3,activation="linear")

    self.conv_2 = Conv2D_BN(filters=filters, kernel_size=1, strides=strides,activation="linear")

  def call(self, x):
    conv_path1 = self.conv_1b(self.conv_1a(x))
    conv_path2 = self.conv_2(x)
    x = k.layers.add([conv_path1, conv_path2])
    return k.activations.relu(x)

class ResIdentityBlockA(k.layers.Layer):
  def __init__(self, filters):
    super(ResIdentityBlockA, self).__init__()
    self.conv_1a = Conv2D_BN(filters=filters, kernel_size=3)
    self.conv_1b = Conv2D_BN(filters=filters, kernel_size=3)
  
  def call(self, x):
    conv_path1 = self.conv_1b(self.conv_1a(x))
    x = k.layers.add([conv_path1, x])
    return k.activations.relu(x)

"""
 For Resnet50/101
"""
class ResConvBlockB(k.layers.Layer):
  def __init__(self, filters, strides=(2,2)):
    super(ResConvBlockB, self).__init__()
    filter_a, filter_b, filter_c = filters
    self.conv_1a = Conv2D_BN(filters=filter_a, kernel_size=1, strides=strides, padding="valid")
    self.conv_1b = Conv2D_BN(filters=filter_b, kernel_size=3)
    self.conv_1c = Conv2D_BN(filters=filter_c, kernel_size=1,activation="linear")

    self.conv_2 = Conv2D_BN(filters=filter_c, kernel_size=1, strides=strides,activation="linear")

  def call(self, x):
    conv_path1 = self.conv_1c(self.conv_1b(self.conv_1a(x)))
    conv_path2 = self.conv_2(x)
    x = k.layers.add([conv_path1, conv_path2])
    return k.activations.relu(x)

class ResIdentityBlockB(k.layers.Layer):
  def __init__(self, filters):
    super(ResIdentityBlockB, self).__init__()
    filter_a, filter_b, filter_c = filters
    self.conv_1a = Conv2D_BN(filters=filter_a, kernel_size=1)
    self.conv_1b = Conv2D_BN(filters=filter_b, kernel_size=3)
    self.conv_1c = Conv2D_BN(filters=filter_c, kernel_size=1, activation="linear")
  
  def call(self, x):
    conv_path1 = self.conv_1c(self.conv_1b(self.conv_1a(x)))
    x = k.layers.add([conv_path1, x])
    return k.activations.relu(x)

"""
  origin ResNet implemention according to 'Deep Residual Learning for Image Recognition'
"""
def ResNet34(input_shape, num_classes):
  inputs = k.Input(shape=input_shape)

  x = Conv2D_BN(filters=64, kernel_size=7, strides=2)(inputs)
  x = MaxPooling2D(pool_size=3, strides=2, padding="same")(x)

  x = ResConvBlockA(filters=64, strides=1)(x)
  for i in range(2):
    x = ResIdentityBlockA(filters=64)(x)

  x = ResConvBlockA(filters=128)(x)
  for i in range(3):
    x = ResIdentityBlockA(filters=128)(x)

  x = ResConvBlockA(filters=256)(x)
  for i in range(5):
    x = ResIdentityBlockA(filters=256)(x)

  x = ResConvBlockA(filters=512)(x)
  for i in range(2):
    x = ResIdentityBlockA(filters=512)(x)
  
  output = GlobalAvgPool2D()(x)
  output = Flatten()(output)
  output = Dense(units=num_classes, activation="softmax")(output)

  return k.Model(inputs=inputs, outputs = output)


def ResNet50(input_shape, num_classes):
  inputs = k.Input(shape=input_shape)

  x = Conv2D_BN(filters=64, kernel_size=7, strides=2)(inputs)
  x = MaxPooling2D(pool_size=3, strides=2, padding="same")(x)

  x = ResConvBlockB(filters=[64,64,256], strides=1)(x)
  for i in range(2):
    x = ResIdentityBlockB(filters=[64,64,256])(x)

  x = ResConvBlockB(filters=[128,128,512])(x)
  for i in range(3):
    x = ResIdentityBlockB(filters=[128,128,512])(x)

  x = ResConvBlockB(filters=[256,256,1024])(x)
  for i in range(5):
    x = ResIdentityBlockB(filters=[256,256,1024])(x)

  x = ResConvBlockB(filters=[512,512,2048])(x)
  for i in range(2):
    x = ResIdentityBlockB(filters=[512,512,2048])(x)
  
  output = GlobalAvgPool2D()(x)
  output = Flatten()(output)
  output = Dense(units=num_classes, activation="softmax")(output)

  return k.Model(inputs=inputs, outputs = output)

def ResNet101(input_shape, num_classes):
  inputs = k.Input(shape=input_shape)

  x = Conv2D_BN(filters=64, kernel_size=7, strides=2)(inputs)
  x = MaxPooling2D(pool_size=3, strides=2, padding="same")(x)

  x = ResConvBlockB(filters=[64,64,256], strides=1)(x)
  for i in range(2):
    x = ResIdentityBlockB(filters=[64,64,256])(x)

  x = ResConvBlockB(filters=[128,128,512])(x)
  for i in range(3):
    x = ResIdentityBlockB(filters=[128,128,512])(x)

  x = ResConvBlockB(filters=[256,256,1024])(x)
  for i in range(22):
    x = ResIdentityBlockB(filters=[256,256,1024])(x)

  x = ResConvBlockB(filters=[512,512,2048])(x)
  for i in range(2):
    x = ResIdentityBlockB(filters=[512,512,2048])(x)
  
  output = GlobalAvgPool2D()(x)
  output = Flatten()(output)
  output = Dense(units=num_classes, activation="softmax")(output)

  return k.Model(inputs=inputs, outputs = output)