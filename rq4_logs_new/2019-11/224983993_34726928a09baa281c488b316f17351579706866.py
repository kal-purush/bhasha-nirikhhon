import torch
import torch.nn as nn

class Discriminator(nn.Module):
    def __init__(self, num_classes = 2):
        super(Discriminator, self).__init__()
        # Convolution 1
        self.conv1 = nn.Sequential(
            nn.Conv2d(3, 64, 4, 2, 1, bias=False),
            nn.LeakyReLU(0.2, inplace=True),
            nn.Dropout(0.5, inplace=False),
        )
        # Convolution 2
        self.conv2 = nn.Sequential(
            nn.Conv2d(64, 64 * 2, 4, 2, 1, bias=False),
            nn.BatchNorm2d(64 * 2),
            nn.LeakyReLU(0.2, inplace=True),
            #nn.Dropout(0.5, inplace=False),
        )
        # Convolution 3
        self.conv3 = nn.Sequential(
            nn.Conv2d(64 * 2, 64 * 4, 4, 2, 1, bias=False),
            nn.BatchNorm2d(64 * 4),
            nn.LeakyReLU(0.2, inplace=True),
            #nn.Dropout(0.5, inplace=False),
        )
        # Convolution 4
        self.conv4 = nn.Sequential(
            nn.Conv2d(64 * 4, 64 * 8, 4, 2, 1, bias=False),
            nn.BatchNorm2d(64 * 8),
            nn.LeakyReLU(0.2, inplace=True),
            #nn.Dropout(0.5, inplace=False),
        )
        # Convolution 5
        self.conv5 = nn.Sequential(
            nn.Conv2d(64 * 8, 1, 4, 1, 0, bias=False),
            nn.Sigmoid(),
        )
        # Convolution 5
        self.conv6 = nn.Sequential(
            nn.Conv2d(64 * 8, num_classes, 4, 1, 0, bias=False),
            nn.Softmax(dim=1),
        )
        # Convolution 6

    def forward(self, input):

        conv1 = self.conv1(input)
        conv2 = self.conv2(conv1)
        conv3 = self.conv3(conv2)
        conv4 = self.conv4(conv3)
        realfake = self.conv5(conv4).view(-1)
        classes = self.conv6(conv4).view(-1,2)
        #classes = self.softmax(fc_aux)
        #realfake = self.sigmoid(fc_dis).view(-1, 1).squeeze(1)
        return realfake, classes