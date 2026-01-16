'''
This demo does the following:

    1. fetch images from the specified search engine
    2. plot images on a grid with labels

'''

from __future__ import division

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.offsetbox import AnchoredText
from matplotlib.patheffects import withStroke
from mpl_toolkits.axes_grid1 import ImageGrid

import Image # PIL
import numpy as np

import cPickle as pickle

import json
import os
import sys

sys.path.append('../..')

from mmfeat.miner import *

'''
Plot grid of images, each word on a row
'''
def gridplot(data_dir, idx, words, out_plot, n_images):
    '''
    data_dir:   directory to load data from
    idx:        the loaded index pickle
    words:      a list of words
    out_plot:   location to write output plot to
    n_images:   number of images per row
    '''

    fig = plt.figure()
    grid = ImageGrid(fig, 111, nrows_ncols=(len(words), n_images), axes_pad=0.0001)

    def add_inner_title(ax, title, loc, size=None, **kwargs):
        if size is None:
            size = dict(size=7) # size=plt.rcParams['legend.fontsize']

        at = AnchoredText(title, loc=loc, prop=size,
                          pad=0., borderpad=0.25,
                          frameon=False, **kwargs)
        ax.add_artist(at)
        at.txt._text.set_path_effects([withStroke(foreground="w", linewidth=3)])
        return at

    count = 0
    for word in words:
        rowcount = 0
        for fn in idx[word]:
            if not os.path.exists('%s/%s' % (data_dir, fn)): continue

            # if you just want the image:
            #img = mpimg.imread('%s/%s' % (data_dir, fn))

            # if you want the image, scaled without maintaining aspect ratio:
            #img = Image.open('%s/%s' % (data_dir, fn) )
            #resized = img.resize((128, 128))
            #img = np.asarray(resized)

            # scaled, centered and maintaining ratio:
            img = Image.open('%s/%s' % (data_dir, fn) )
            width, height = img.size[0], img.size[1]
            padSz = 2
            if width > height:
                n_height = int((height / width) * 100)
                resized = img.resize((100, n_height))
                img = np.asarray(resized)

                # center
                pad_height = int((100 - n_height) / 2)
                npad = ((padSz+pad_height, padSz+pad_height), (padSz, padSz), (0,0))
            elif height > width:
                n_width = int((width / height) * 100)
                resized = img.resize((n_width, 100))
                img = np.asarray(resized)

                # center
                pad_width = int((100 - n_width) / 2)
                npad = ((padSz,padSz), (padSz+pad_width, padSz+pad_width), (0,0))
            else:
                resized = img.resize((100, 100))
                img = np.asarray(resized)
                npad = ((padSz, padSz), (padSz, padSz), (0,0))

            img = np.pad(img, pad_width=npad, mode='constant', constant_values=255)

            # add to grid
            fr = grid[count]
            fr.imshow(img)

            # remove ticks and give label
            fr.axes.get_xaxis().set_ticks([])
            fr.axes.get_yaxis().set_ticks([])
            add_inner_title(fr, '%s %d' % (word, rowcount+1), loc=3)

            count += 1
            rowcount += 1
            if rowcount == n_images:
                rowcount = 0
                break

    # save
    plt.axis('off')
    plt.savefig(out_plot, bbox_inches='tight')
    plt.clf()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python %s {google|bing|flickr} out_plot')
        quit()

    engine = sys.argv[1]
    out_plot = sys.argv[2]

    #
    # 0. Set up data
    #
    data_dir = './demo-data-%s' % engine
    words = ['dog', 'golden retriever']
    if engine == 'esp':
        words = ['dog']
    elif engine == 'imagenet':
        words = ['golden retriever']
    n_images = 5

    #
    # 1. Fetch images
    #
    print('Fetching images..')
    if not os.path.exists(data_dir):
        if engine == 'bing':
            miner = BingMiner(data_dir, '../../miner.yaml')
        elif engine == 'google':
            miner = GoogleMiner(data_dir, '../../miner.yaml')
        elif engine == 'flickr':
            miner = FlickrMiner(data_dir, '../../miner.yaml')
        miner.getResults(words, 10)
        miner.save()
    else:
        print('Image directory already exists..')

    #
    # 2. Plot
    #
    idx = pickle.load(open(data_dir + '/index.pkl'))
    gridplot(data_dir, idx, words, out_plot, n_images)