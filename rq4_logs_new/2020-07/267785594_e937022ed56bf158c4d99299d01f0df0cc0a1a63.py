import tensorflow as tf
from tensorflow.keras.layers import *
import tensorflow_addons as tfa


class I2I_AE:
    def __init__(self):
        self.img_size = 28
        self.img_channels = 1
        self.num_classes = 10
        self.latent_dim = 100
        self.img_shape = (self.img_size, self.img_size, self.img_channels)

    def build_encoder(self):
        def conv2d(layer_input, filters, kernel_size, strides, activation='relu'):
            x = Conv2D(filters=filters, kernel_size=kernel_size, strides=strides, padding="same")(layer_input)
            x = BatchNormalization(momentum=0.8)(x)
            x = Activation(activation)(x)
            return x

        def upconv2d(layer_input, filters, kernel_size, strides, activation='relu'):
            x = Conv2DTranspose(filters=filters, kernel_size=kernel_size, strides=strides, padding="same")(layer_input)
            x = BatchNormalization(momentum=0.8)(x)
            x = Activation(activation)(x)
            return x

        stacked_input = Input(shape=(self.img_size, self.img_size, 2))

        encoder = conv2d(stacked_input, 64, 3, 2)
        encoder = conv2d(encoder, 128, 3, 2)
        encoder = conv2d(encoder, 256, 3, 1)
        encoder = conv2d(encoder, 512, 3, 1)
        encoder = conv2d(encoder, 1024, 3, 1)

        decoder = upconv2d(encoder, 1024, 3, 1)
        decoder = upconv2d(decoder, 512, 3, 1)
        decoder = upconv2d(decoder, 256, 3, 1)
        decoder = upconv2d(decoder, 128, 3, 2)
        encoder_out = upconv2d(decoder, 1, 3, 2, activation='sigmoid')

        return tf.keras.Model(inputs=stacked_input, outputs=encoder_out)

    def build_decoder(self):
        def conv2d(layer_input, filters, kernel_size, strides, activation='relu'):
            x = Conv2D(filters=filters, kernel_size=kernel_size, strides=strides, padding="same")(layer_input)
            x = BatchNormalization(momentum=0.8)(x)
            x = Activation(activation)(x)
            return x

        def upconv2d(layer_input, filters, kernel_size, strides, activation='relu'):
            x = Conv2DTranspose(filters=filters, kernel_size=kernel_size, strides=strides, padding="same")(layer_input)
            x = BatchNormalization(momentum=0.8)(x)
            x = Activation(activation)(x)
            return x

        composed_image = Input(shape=(self.img_size, self.img_size, 1))

        encoder = conv2d(composed_image, 64, 3, 2)
        encoder = conv2d(encoder, 128, 3, 2)
        encoder = conv2d(encoder, 256, 3, 1)
        encoder = conv2d(encoder, 512, 3, 1)
        encoder = conv2d(encoder, 1024, 3, 1)

        decoder = upconv2d(encoder, 1024, 3, 1)
        decoder = upconv2d(decoder, 512, 3, 1)
        decoder = upconv2d(decoder, 256, 3, 1)
        decoder = upconv2d(decoder, 128, 3, 2)
        decoder_out = upconv2d(decoder, 2, 3, 2, activation='sigmoid')

        return tf.keras.Model(inputs=composed_image, outputs=decoder_out)

    def build_autoencoder(self, encoder_net, decoder_net):
        stacked_input = Input(shape=(self.img_size, self.img_size, 2))

        composed_image = encoder_net(stacked_input)
        reconstructed_image = decoder_net(composed_image)

        return tf.keras.Model(inputs=composed_image, outputs=reconstructed_image)