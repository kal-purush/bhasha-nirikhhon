import numpy as np
import torch as t
from torch import nn
from torch.nn import functional as F

DefaultSettings = CondColorSettings()

class CondColorSettings:
    def __init__(self):
        self.is_eval = False

class ColorEncoder(nn.Module):
    def __init__(self, height, width, nchannels = 3, nfilters = 3, kernel_size = 4, stride = 3, padding = 0, settings = DefaultSettings):
        super(Model, self).__init__()
        self.settings = settings, self.height = height, self.width
        self.latentsize = 7

        # calculate heigh, width, after convolution to ensure validity
        outheight = ((height + (2*padding) - kernel_size) / stride) + 1.
        outwidth = ((width + (2*padding) - kernel_size) / stride) + 1. 

        if not self.settings.is_eval:
            assert outheight.is_integer(), "Height after conv1 would not be valid!"
            assert outwidth.is_integer(), "Width after conv1 would not be valid!"

        # run one conv layer over the entire image to gather colors
        self.conv1 = nn.Conv2d(nchannels, nfilters, stride, padding)

        # fully connected layers to map to embedding
        self.fc1 = nn.Linear(nchannels * nfilters * outheight * outwidth, 20)
        self.fc21 = nn.Linear(20, self.latentsize)
        self.fc22 = nn.Linear(20, self.latentsize)

    # input is image of height, width, nchannels
    # output is vector of dimension 7 (atm)
    def encode(self, x):
        # check for valid input
        if not self.settings.is_eval:
            assert list(t.shape) == [self.height, self.width, self.nchannels]
    
        # apply layers
        h1 = self.conv1(input)
        h2 = F.relu(h1)
        h3 = self.fc1(h2)
        h4 = F.relu(h3)
        h51 = self.fc21(h4)
        h52 = self.fc22(h4)

        return h51, h52

    # VAE reparameterization trick to help us eventually sample from latent space
    def reparameterize(self, mu, logvar):
        
        std = t.exp(0.5*logvar)
        eps = t.randn_like(std)
        return mu + eps*std

    def forward(self, input):
        
        # transform and encode input
        mu, logvar = self.encode(input.view(-1, self.height, self.width, self.nchannels))
        z = self.reparameterize(mu, logvar)
        return z

    # sample from latent space
    def sample(self):
        
        
