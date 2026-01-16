import tensorflow as tf

class Block(tf.keras.Model):
    def __init__(self, filters, kernel_size, repetitions, pool_size = 2, strides = 2):
        super(Block, self).__init__()
        self.filters = filters
        self.kernel_size =kernel_size
        self.repetitions = repetitions

        for i in range(0, repetitions):
            vars(self)[f'conv2D_{i}'] = tf.keras.layers.Conv2D(filters, kernel_size, activation = 'relu', padding = 'same')
        
        self.max_pool = tf.keras.layers.MaxPool2D(kernel_size, strides)

    def call(self, inputs):
        conv2D_0 = vars(self)['conv2D_0']
        
        x = conv2D_0(inputs)

        for i in range(1, self.repetitions):
            conv2D_i = vars(self)[f'conv2D_{i}']

            x = conv2D_i(x)
        max_pool = self.max_pool(x)

        return x