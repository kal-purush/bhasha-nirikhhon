'''
this implementation using batch norm
'''

import torch
import torch.nn as nn


##我们使用string max-pool的原因是，下面一旦遇到这个str max-pool,就使用max-pool technique1,也就是2*2-s-2的maxpool layer
yolov1_cnn_architecture = [
    ##尝试去解释这个padding size
    #(kernel_size, num_filters,stride, padding)
    (7, 64, 2, 3),
    "Max-pool",
    (3, 192, 1, 1),
    "Max-pool",
    (1, 128, 1, 0),
    (3, 256, 1, 1),
##不太理解这个作用，我们猜想是为了提高可训练性，但对shape没有影响
    (1, 256, 1, 0),
    (3, 512, 1, 1),
    "Max-pool",
    [(1, 256, 1, 0), (3, 512, 1, 1), 4],
    (1, 512, 1, 0),
    (3, 1024, 1, 1),
    "Max-pool",
    [(1, 512, 1, 0), (3, 1024, 1, 1), 2],
    (3, 1024, 1, 1),
    (3, 1024, 2, 1),
    (3, 1024, 1, 1),
    (3, 1024, 1, 1),
] 

class CNNBlock(nn.Module):
    def __init__(self, in_channels, out_channels, **kwargs):
        #这个super不知道干嘛的
        super(CNNBlock,self).__init__()
        self.conv = nn.Conv2d(in_channels,out_channels,bias=False,**kwargs)
        self.batchnorm = nn.BatchNorm2d(out_channels)
        ##这个的好处
        self.leakyrelu = nn.LeakyReLU(0.1)

    def forward(self, x):
        return self.leakyrelu(self.batchnorm(self.conv(x)))

class Yolov1(nn.Module):
    #这个in_channels = 3的原因是我们使用RGB images
    def __init__(self, in_channels=3, **kwargs):
        super(Yolov1, self).__init__()
        self.architecture = yolov1_cnn_architecture
        self.in_channels = in_channels
        self.darknet = self._create_conv_layers(self.architecture)
        self.fcs = self._create_fcs(**kwargs)

    def forward(self,x):
        x = self.darknet(x)
        return self.fcs(torch.flatten(x, start_dim=1))

    def _create_conv_layers(self, architecture):
        layers = []
        in_channels = self.in_channels

        for x in architecture:
            if type(x) == tuple:
                layers += [
                    CNNBlock(
                        in_channels,
                        x[1],
                        kernel_size = x[0],
                        stride = x[2],
                        padding = x[3]
                    )
                ]
                in_channels = x[1]
            elif type(x) == str:
                layers += [
                    nn.MaxPool2d(kernel_size=2,stride=2)
                ]
            elif type(x) == list:
                conv1 = x[0]
                conv2 = x[1]
                num_repeats = x[2]

                for _ in range(num_repeats):
                    layers += [
                        CNNBlock(
                            in_channels,
                            conv1[1],
                            kernel_size = conv1[0],
                            stride = conv1[2],
                            padding = conv1[3]

                        )
                    ]
                    layers += [
                        CNNBlock(
                            conv1[1],
                            conv2[1],
                            kernel_size = conv2[0],
                            stride = conv2[2],
                            padding = conv2[3]

                        )
                    ]

                    in_channels = conv2[1]
        return nn.Sequential(*layers)


    def _create_fcs(self, split_size, num_boxes, num_classes):
        S, B, C = split_size, num_boxes, num_classes
        return nn.Sequential(
            nn.Flatten(),
            nn.Linear(1024 * S * S, 496), # in paper, it should be 4096
            nn.Dropout(0.0),
            nn.LeakyReLU(0.1),
            nn.Linear(496, S * S * (C + B*5)) 
        )

def test(S = 7, B=2, C=20):
    model = Yolov1(split_size=S,num_boxes=B,num_classes=C)
    x = torch.randn((2, 3, 448, 448))
    print(model(x).shape)

test()
    


