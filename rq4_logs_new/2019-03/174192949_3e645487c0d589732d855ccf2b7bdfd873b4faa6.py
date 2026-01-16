#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  6 15:53:52 2017

@author: Jay Bassan

Useful processing functions for IMC with tellurium probes.
"""
import numpy as np                     # array manipulation
import pandas as pd                    # fast loading of txt files
from matplotlib import pyplot as plt   # saving images
from skimage import filters            # 2D Gaussian blur
import os                              # making directories to store images
from scipy import ndimage              # 3D Gaussian blur
import sys                             # printing warnings to user


def clamp(img, percentile, normalize=False):
    """
    Takes an image and clamps any value above the specified percentile to
    that value, with optional normalization to 1.
    Useful for getting rid of the few very bright pixels that skew the image.
    """
    img_ = np.copy(img)
    img_[img_ > np.percentile(img_, percentile)] = np.percentile(img_, percentile)
    if normalize:
        out = img_ / img_.max()
        return out
    elif not normalize:
        out = img_
        return out
    else:
        raise ValueError('Normalize must be either True or False.')


def txt_to_npy(txt_file, preview_imgs=True, cmap='gray'):
    """
    Converts standard Fluidigm txt file to a series of numpy arrays.
    Corrects for badly shaped data -- i.e. images where the ablation was
    stopped halfway across a row.
    """
    sys.stdout.write('Reading ' + txt_file + '\n')
    sys.stdout.flush()

    init_import = pd.read_csv(txt_file, sep='\t')
    init_array = init_import.values
    x_size = init_import['X'].max() + 1
    y_size = init_import['Y'].max() + 1
    z_size = init_array.shape[1]

    sys.stdout.write('Data read successfully.\n')
    sys.stdout.flush()
    """
    Checks to see if the data has a bad shape.
    """
    if (x_size * y_size) == init_array.shape[0]:
        bad_shape = False
    else:
        bad_shape = True
        sys.stdout.write('Bad shape.  Will clip last raster line.\n')
        sys.stdout.write('Can happen if ablation is stopped halfway through a row.\n')
        sys.stdout.flush()

    """
    Makes a directory, clips if bad shape, and saves.  If preview images,
    saves a clamped blurred png image for each channel with colour map and
    axes.  Prints progress.
    """
    os.mkdir(txt_file[:-4])
    if bad_shape:
        y_size -= 1
        delta = init_array.shape[0] - (x_size * y_size)
        clipped = init_array[:-delta]
        full_image = np.reshape(clipped, [y_size, x_size, z_size], order='C')
        for i in range(6, z_size):
            sys.stdout.write('\rSaving channel {0} of {1}. '.format(i-5, z_size-6) + '[' + '-'*(i-5) + ' ' * (z_size-i-1) + ']')
            if i == z_size - 1:
                sys.stdout.write('\n')
            sys.stdout.flush()
            np.save(os.path.join(txt_file[:-4], init_import.columns.values[i]), full_image[:, :, i])
            if preview_imgs:
                plt.imshow(filters.gaussian(clamp(full_image[:, :, i], 99), sigma=1), cmap=cmap)
                plt.colorbar()
                plt.savefig(os.path.join(txt_file[:-4], init_import.columns.values[i]) + '_preview.png', dpi=600)
                plt.clf()
    else:
        full_image = np.reshape(init_array, [y_size, x_size, z_size], order='C')
        for i in range(6, z_size):
            sys.stdout.write('\rSaving channel {0} of {1}. '.format(i-5, z_size-6) + '[' + '-'*(i-5) + ' ' * (z_size-i-1) + ']')
            if i == z_size - 1:
                sys.stdout.write('\n')
            sys.stdout.flush()
            np.save(os.path.join(txt_file[:-4], init_import.columns.values[i]), full_image[:, :, i])
            if preview_imgs:
                plt.imshow(filters.gaussian(clamp(full_image[:, :, i], 99), sigma=1), cmap=cmap)
                plt.colorbar()
                plt.savefig(os.path.join(txt_file[:-4], init_import.columns.values[i]) + '_preview.png', dpi=600)
                plt.clf()


def grey2red(img):
    """
    Transforms a greyscale image to red.
    """
    out = np.dstack((img, np.zeros_like(img), np.zeros_like(img)))
    if img.max() > 1:
        warning = 'Warning: input image is not normalized to 1.\n\
        This did not affect the data processing but colour images need to be\n\
        either floats between 0 and 1 or integers between 0 and 255.\n'
        sys.stdout.write(warning)
        sys.stdout.flush()
    return out


def grey2green(img):
    """
    Transforms a greyscale image to green.
    """
    out = np.dstack((np.zeros_like(img), img, np.zeros_like(img)))
    if img.max() > 1:
        warning = 'Warning: input image is not normalized to 1.\n\
        This did not affect the data processing but colour images need to be\n\
        either floats between 0 and 1 or integers between 0 and 255.\n'
        sys.stdout.write(warning)
        sys.stdout.flush()
    return out


def grey2blue(img):
    """
    Transforms a greyscale image to blue.
    """
    out = np.dstack((np.zeros_like(img), np.zeros_like(img), img))
    if img.max() > 1:
        warning = 'Warning: input image is not normalized to 1.\n\
        This did not affect the data processing but colour images need to be\n\
        either floats between 0 and 1 or integers between 0 and 255.\n'
        sys.stdout.write(warning)
        sys.stdout.flush()
    return out


def grey2cyan(img):
    """
    Transforms a greyscale image to cyan.
    Remember to multiply by 0.5 in any final CYM compilation.
    """
    return grey2green(img) + grey2blue(img)


def grey2yellow(img):
    """
    Transforms a greyscale image to yellow.
    Remember to multiply by 0.5 in any final CYM compilation.
    """
    return grey2red(img) + grey2green(img)


def grey2magenta(img):
    """
    Transforms a greyscale image to magenta.
    Remember to multiply by 0.5 in any final CYM compilation.
    """
    return grey2red(img) + grey2blue(img)


def remove_xenon(te_img, xe_img, te_iso, xe_iso, clamp_neg=True):
    """
    Based on the abundances of the Xe and Te isotopes in question, and the Xe
    images, calculates the expected component the Te image that actually comes
    from Xe and subtracts it.
    By default the function assumes any pixels that become negative after
    subtraction contain no tellurium and so clamps them to 0.
    """
    # Percentage abundance of different Xe isotopes
    xe_abundances = {'124': 0.095,
                     '126': 0.089,
                     '128': 1.910,
                     '129': 26.401,
                     '130': 4.071,
                     '131': 21.232,
                     '132': 26.909,
                     '134': 10.436,
                     '136': 8.857}
    """
    Checks that the isotope requested is xenon contaminated.
    Returns the input array if not, and prints a warning.
    """
    if str(te_iso)not in xe_abundances:
        print('{0} is not contaminated with Xe.  Input image returned.'.format(str(te_iso)))
        return te_img

    ratio = xe_abundances[str(te_iso)] / xe_abundances[str(xe_iso)]

    scaled_xe = xe_img * ratio

    subtracted = te_img - scaled_xe

    # Clamp negative pixels to zero if clamp_neg
    if clamp_neg:
        subtracted[subtracted < 0] = 0

    return subtracted


def get_vals(img1, img2, thresh):
    """
    * * * Intended as an internal function. * * *
    Speeds up getting ratio of two images where both images have a non zero
    value for any given pixel.
    """
    mask1 = img1 > thresh
    mask2 = img2 > thresh
    mask = np.logical_not(mask1 & mask2)

    masked1 = np.ma.masked_array(img1, mask=mask)
    masked2 = np.ma.masked_array(img2, mask=mask)

    return masked1.compressed(), masked2.compressed()


def calculate_teb(img1, img2, iso1, iso2, thresh=0, force_origin=False, plot=False, bins=256):
    """
    Takes two tellurium images and shows an isotopic scatter plot.  The thresh
    allows ignoring of values that are very low in either channel.
    """
    te_dict = {'120':0.09,
               '122':2.55,
               '123':0.89,
               '124':4.74,
               '125':7.07,
               '126':18.84,
               '128':31.74,
               '130':34.08}
    
    siso1 = str(iso1)
    siso2 = str(iso2)

    th_ratio = te_dict[siso2] / te_dict[siso1]

    vals = get_vals(img1, img2, thresh=thresh)
    ratio_distribution = vals[1] / vals[0]

    calc_ratio = np.median(ratio_distribution)
    calc_std = np.std(ratio_distribution)

    if plot:
        fig, ax = plt.subplots(ncols=2)

        ax[0].hist(ratio_distribution, bins=256, color=[0.6, 0.6, 0.6])
        ax[0].set_title('1D histogram')
        ax[0].set_xlabel(siso2 + '/' + siso1 + ' ratio')
        ax[0].set_ylabel('Number of pixels')
        ax[0].axvline(th_ratio, c='r', linestyle='--')
        ax[0].axvline(calc_ratio, c='g')

        xlim = np.percentile(vals[0], 99)
        ylim = np.percentile(vals[1], 99)
        ax[1].hexbin(vals[0], vals[1], cmap='binary', bins=bins)
        ax[1].plot([0,xlim],
                 [0,xlim*th_ratio],
                 c='r',
                 linestyle='--',
                 label='th. ratio = {0}'.format(round(th_ratio, 2)))
    
        ax[1].plot([0,xlim],
                 [0,xlim*calc_ratio],
                 c='g',
                 label='calc. ratio = {0}'.format(round(calc_ratio, 2)))

        ax[1].set_xlim(0,xlim)
        ax[1].set_ylim(0,ylim)
        ax[1].set_title('2D histogram')
        ax[1].set_xlabel(siso1)
        ax[1].set_ylabel(siso2)
        plt.legend()
        plt.tight_layout()
        plt.show()
        return {'th. ratio ' + siso2 + '/' + siso1: th_ratio,
                'calc. ratio': calc_ratio,
                'calc. std':calc_std,
                'TEB': calc_ratio/th_ratio,
                'TEB std':calc_std / th_ratio}        
    else:
        return {'th. ratio ' + siso2 + '/' + siso1: th_ratio,
                'calc. ratio': calc_ratio,
                'calc. std':calc_std,
                'TEB': calc_ratio/th_ratio,
                'TEB std':calc_std / th_ratio}


def combine_channels(imgs, mode, xyz_blur=False, sigma=None):
    """
    Takes an interable of tellurium images (must be same ablation!) and
    combines them.  Returns one image of the same shape as each image in the
    iterable.

    Arguments
    --------
    imgs:
        an iterable (tuple, list or ndarray) where each item is an image the
        user wants to include in the final combined image.
    mode:
        'arithmetic':
            combines the images in an arithmetic fashion
        'geometric':
            combines the images in a geometric fashion
    xyz_blur:
        If True, performs a 3D Gaussian blur on the images in a stack.
    sigma:
        If xyz_blur is requested, a sigma value must be provided.  Will warn
        users who request the blur but forget to provide a sigma value.

    Returns
    --------
    Image as ndarray.  Will be same size as the input images.
    """
    stack = np.dstack(imgs)
    if xyz_blur:
        if sigma is None:
            sys.stdout.write('You must provide a sigma value for the blur.')
            sys.stdout.flush()
            return None
        else:
            stack = ndimage.filters.gaussian_filter(stack, sigma=sigma)

    if mode == 'arithmetic':
        collapsed = np.sum(stack, axis=2)
        collapsed = collapsed / len(imgs)
    elif mode == 'geometric':
        collapsed = np.product(stack, axis=2)
        collapsed = collapsed ** (1/len(imgs))
    else:
        sys.stdout.write('Invalid argument for mode: {0}\n'.format(mode))
        sys.stdout.write('Please specify either \'arithmetic\' or \'geometric\'.\n')
        return None

    return collapsed