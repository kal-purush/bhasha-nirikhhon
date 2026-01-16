import tensorflow as tf
from tensorflow.keras.layers import Conv2D, Activation, Concatenate


class FusionBlock(tf.keras.layers.Layer):
    def __init__(self, c_feat, c_alpha=1):
        super(FusionBlock, self).__init__()
        c_img = 3
        self.map2img = tf.keras.Sequential([
            Conv2D(c_img, 1, 1, padding='same'),
            Activation('sigmoid')
        ])
        # Assuming BlendBlock is defined elsewhere
        self.blend = BlendBlock(c_img * 2, c_alpha)

    def call(self, img_miss, feat_de):
        img_miss = tf.image.resize(img_miss, feat_de.shape[1:3])
        raw = self.map2img(feat_de)
        alpha = self.blend(tf.concat([img_miss, raw], axis=3))
        result = alpha * raw + (1 - alpha) * img_miss
        return result, alpha, raw


class BlendBlock(tf.keras.layers.Layer):
    def __init__(self, c_in, c_out, ksize_mid=3, norm='batch', act='relu'):
        super(BlendBlock, self).__init__()
        c_mid = max(c_in // 2, 32)
        self.blend = tf.keras.Sequential([
            Conv2D(c_mid, 1, 1, padding='same'),
            Activation('relu'),
            Conv2D(c_out, ksize_mid, 1, padding='same'),
            Activation('relu'),
            Conv2D(c_out, 1, 1, padding='same'),
            Activation('sigmoid')
        ])

    def call(self, x):
        return self.blend(x)


class DecodeBlock(tf.keras.layers.Layer):
    def __init__(self, c_from_up, c_from_down, c_out, mode='nearest',
                 kernel_size=4, scale=2, normalization='batch', activation='relu'):
        super(DecodeBlock, self).__init__()

        self.c_from_up = c_from_up
        self.c_from_down = c_from_down
        self.c_in = c_from_up + c_from_down
        self.c_out = c_out

        self.up = UpSampling2D(size=(scale, scale), interpolation=mode)

        layers = []
        layers.append(
            Conv2D(self.c_out, kernel_size, strides=1, padding='same'))
        if normalization:
            # Assuming get_norm is defined elsewhere
            layers.append(get_norm(normalization, self.c_out))
        if activation:
            layers.append(Activation(activation))
        self.decode = tf.keras.Sequential(layers)

    def call(self, x, concat=None):
        out = self.up(x)
        if self.c_from_down > 0:
            out = Concatenate(axis=-1)([out, concat])
        out = self.decode(out)
        return out


from tensorflow.keras.layers import Conv2D, Activation


class EncodeBlock(tf.keras.layers.Layer):
    def __init__(self, in_channels, out_channels, kernel_size, stride,
                 normalization=None, activation=None):
        super(EncodeBlock, self).__init__()

        self.c_in = in_channels
        self.c_out = out_channels

        layers = []
        layers.append(
            Conv2D(self.c_out, kernel_size, strides=stride, padding='same'))
        if normalization:
            layers.append(get_norm(normalization, self.c_out))
        if activation:
            layers.append(Activation(activation))
        self.encode = tf.keras.Sequential(layers)

    def call(self, x):
        return self.encode(x)


from tensorflow.keras.layers import Conv2DTranspose, UpSampling2D


class UpBlock(tf.keras.layers.Layer):
    def __init__(self, mode='nearest', scale=2, channel=None, kernel_size=4):
        super(UpBlock, self).__init__()

        self.mode = mode
        if mode == 'deconv':
            self.up = Conv2DTranspose(channel, kernel_size, strides=scale, padding='same')
        else:
            def upsample(x):
                return tf.image.resize(x, size=(x.shape[1] * scale, x.shape[2] * scale), method=mode)

            self.up = upsample

    def call(self, x):
        return self.up(x)


from tensorflow.keras.layers import Conv2DTranspose


class ConvTranspose2dSame(tf.keras.layers.Layer):
    def __init__(self, in_channels, out_channels, kernel_size, stride):
        super(ConvTranspose2dSame, self).__init__()

        padding, output_padding = self.deconv_same_pad(kernel_size, stride)
        self.trans_conv = Conv2DTranspose(out_channels, kernel_size, strides=stride, padding='same',
                                          output_padding=output_padding)

    def deconv_same_pad(self, ksize, stride):
        pad = (ksize - stride + 1) // 2
        outpad = 2 * pad + stride - ksize
        return pad, outpad

    def call(self, x):
        return self.trans_conv(x)


from tensorflow.keras.layers import Conv2D, ZeroPadding2D
from tensorflow.keras import Sequential


class Conv2dSame(tf.keras.layers.Layer):
    def __init__(self, in_channels, out_channels, kernel_size, stride):
        super(Conv2dSame, self).__init__()

        padding = self.conv_same_pad(kernel_size, stride)
        if isinstance(padding, int):
            self.conv = Conv2D(out_channels, kernel_size, strides=stride, padding='same')
        else:
            self.conv = Sequential([
                ZeroPadding2D(padding),
                Conv2D(out_channels, kernel_size, strides=stride, padding='valid')
            ])

    def conv_same_pad(self, ksize, stride):
        if (ksize - stride) % 2 == 0:
            return (ksize - stride) // 2
        else:
            left = (ksize - stride) // 2
            right = left + 1
            return (left, right)

    def call(self, x):
        return self.conv(x)


from tensorflow.keras.layers import ReLU, ELU, LeakyReLU, Activation


def get_activation(name):
    if name == 'relu':
        activation = ReLU()
    elif name == 'elu':
        activation = ELU()
    elif name == 'leaky_relu':
        activation = LeakyReLU(alpha=0.2)
    elif name == 'tanh':
        activation = Activation('tanh')
    elif name == 'sigmoid':
        activation = Activation('sigmoid')
    else:
        activation = None
    return activation


from tensorflow.keras.layers import BatchNormalization, LayerNormalization


def get_norm(name, out_channels):
    if name == 'batch':
        norm = BatchNormalization()
    elif name == 'instance':
        norm = LayerNormalization()
    else:
        norm = None
    return norm


import tensorflow as tf
from tensorflow.keras.layers import Conv2D, Activation, Concatenate


class DFNet(tf.keras.Model):
    def __init__(
            self, c_img=3, c_mask=1, c_alpha=3,
            mode='nearest', norm='batch', act_en='relu', act_de='relu',
            en_ksize=[7, 5, 5, 3, 3, 3, 3, 3], de_ksize=[3] * 8,
            blend_layers=[0, 1, 2, 3, 4, 5], **kwargs):
        super(DFNet, self).__init__(**kwargs)

        c_init = c_img + c_mask

        self.n_en = len(en_ksize)
        self.n_de = len(de_ksize)
        assert self.n_en == self.n_de, (
            'The number of layers in Encoder and Decoder must be equal.')
        assert self.n_en >= 1, (
            'The number of layers in Encoder and Decoder must be greater than 1.')

        assert 0 in blend_layers, 'Layer 0 must be blended.'

        self.en = []
        c_in = c_init
        self.en.append(
            EncodeBlock(c_in, 64, en_ksize[0], 2, None, None))
        for k_en in en_ksize[1:]:
            c_in = self.en[-1].c_out
            c_out = min(c_in * 2, 512)
            self.en.append(EncodeBlock(
                c_in, c_out, k_en, stride=2,
                normalization=norm, activation=act_en))

        self.de = []
        self.fuse = []
        for i, k_de in enumerate(de_ksize):
            c_from_up = self.en[-1].c_out if i == 0 else self.de[-1].c_out
            c_out = c_from_down = self.en[-i - 1].c_in
            layer_idx = self.n_de - i - 1

            self.de.append(DecodeBlock(
                c_from_up, c_from_down, c_out, mode, k_de, scale=2,
                normalization=norm, activation=act_de))
            if layer_idx in blend_layers:
                self.fuse.append(FusionBlock(c_out, c_alpha))
            else:
                self.fuse.append(None)

        # register parameters
        for i, de in enumerate(self.de[::-1]):
            self.__setattr__('de_{}'.format(i), de)
        for i, fuse in enumerate(self.fuse[::-1]):
            if fuse:
                self.__setattr__('fuse_{}'.format(i), fuse)

    def call(self, img_miss, mask):
        out = tf.concat([img_miss, mask], axis=3)

        out_en = [out]
        for encode in self.en:
            out = encode(out)
            out_en.append(out)

        results = []
        alphas = []
        raws = []
        for i, (decode, fuse) in enumerate(zip(self.de, self.fuse)):
            out = decode(out, out_en[-i - 2])
            if fuse:
                result, alpha, raw = fuse(img_miss, out)
                results.append(result)
                alphas.append(alpha)
                raws.append(raw)

        return results[::-1], alphas[::-1], raws[::-1]


if __name__ == '__main__':
    print('hello world')

    # model = DFNet(inputs= tf.random.normal(shape=(2, 3, 4, 5)), outputs=tf.random.normal(shape=(3, 2)))
    model = DFNet()
    
    # model = tf.keras.Sequential([tf.keras.layers.Dense(500)])
    model.compile(
        loss=tf.keras.losses.BinaryCrossentropy(),
        metrics=[
            tf.keras.metrics.BinaryAccuracy(),
            tf.keras.metrics.FalseNegatives(),
        ],

    )
    model.build((None, None, None, 3))
    model.summary()