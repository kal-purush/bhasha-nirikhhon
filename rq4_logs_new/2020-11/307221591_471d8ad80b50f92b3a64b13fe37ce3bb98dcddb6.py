import sys

import numpy as np

sys.path.append('./dnn')
import neural_network as nn
import activationfunction as af
import setting

class Convolution_Layer:

    def __init__(self,
                 data,
                 channel,
                 kernel_size,
                 k_method="xivier",
                 stride=1,
                 actfunc="sigmoid",
                 padding_method="valid-method"):
        self.data = data
        self.channel = channel
        self.kernel_size = kernel_size
        self.stride = stride
        self.actfunc = actfunc
        self.padding_method = padding_method
        self.__select_w(k_method)

    def __select_w(self, k_method):
        if k_method == "xivier":
            self.kernel = setting.knet(self.channel, self.kernel_size, setting.xivier)
        elif k_method == "he":
            self.kernel = setting.knet(self.channel, self.kernel_size, setting.he)
        else:
            sys.stdout.write("Error: The kernel method is not found\n")
            sys.exit(1)

    def padding(self):
        #padding
        if self.padding_method == "valid-padding":
            x = x
            padding_size = 0
            data_size += padding_size
        else:
            sys.stdout.write("Error: The padding method is not found\n")
            sys.exit(1)

    def convolution(self):
        data_size = len(self.data)
        # make output array
        if (data_size - self.kernel_size) % self.stride == 0:
            out_size = int((data_size - self.kernel_size) / self.stride + 1)
            out = np.zeros([self.channel, out_size, out_size])
        else:
            sys.stdout.write("Error: The stride is not right\n")
            sys.exit(1)
        #convolution
        for h in range(self.channel):
            for i in range(out_size):
                for j in range(out_size):
                    y = (self.data[i:i + self.kernel_size].T[j:j + self.kernel_size].T) @ self.kernel[h]
                    z = af.sigmoid(y)
                    out[h][i][j] = np.sum(z)
        return out