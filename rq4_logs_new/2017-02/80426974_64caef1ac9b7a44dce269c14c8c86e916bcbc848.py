from vgg17 import VGG17
from keras import backend as K
from keras.utils.np_utils import convert_kernel
import tensorflow as tf

model = VGG17()
model.load_weights()
ops = []
for layer in model.layers:
   if layer.__class__.__name__ in ['Convolution1D', 'Convolution2D', 'Convolution3D', 'AtrousConvolution2D']:
      original_w = K.get_value(layer.W)
      converted_w = convert_kernel(original_w)
      ops.append(tf.assign(layer.W, converted_w).op)

K.get_session().run(ops)

model.save_weights('my_weights_tensorflow.h5')