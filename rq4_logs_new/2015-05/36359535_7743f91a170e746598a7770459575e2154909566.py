# monoraw2img.py
# author: tiwawan (twitter:@lab_rad_or, github:tiwawan)
# Python version 3.4.3
# Last Update: 2015/05/27
# Since: 2015/05/27

"""convert monochromatic RAW image to normal image(png, jpg, etc.)."""


import sys
import numpy as np
#import scipy as sp
import matplotlib.pyplot as plt
import matplotlib.image as img
from struct import *




class MonoRaw2Img:
    """MonoRaw2Img:a class to convert monochromatic RAW image to normal image\n
    iwidth  : width of RAW image\n
    iheight : height of RAW image\n
    ibytes  : bytes per 1 pixel\n
    cmap    : color map(see matplotlib reference)\n
    """
    def __init__(self, iwidth=1280, iheight=1024, ibytes=4, icmap='hot'):
        self.width = int(iwidth)
        self.height = int(iheight)
        self.bytes = int(ibytes)
        self.cmap = icmap

    def setWidth(self, w):
        width = w

    def setHeight(self, h):
        height = h

    def setBytes(self, b):
        bytes = b

    def setCmap(self, c):
        cmap = c
    
    def convert(self, inputname, outputname):
        rimg = open(inputname, mode = 'rb')
        imgmat = [0]*self.height
        for y in range(0, self.height):
            imgmat[y] = []
            for x in range(0, self.width):
                dot = unpack('L', rimg.read(self.bytes))[0]
                imgmat[y].append(dot);
        imgmat_np = np.matrix(imgmat)
        map = plt.imshow(imgmat_np, interpolation='none')
        map.set_cmap(self.cmap)
        img.imsave(outputname, imgmat_np, cmap=self.cmap)
        plt.show()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: python monoraw2img.py input output width height bytes cmap')
    else:
        conv = MonoRaw2Img(*sys.argv[3:])
        conv.convert(sys.argv[1], sys.argv[2])
    