__author__ = 'phoenix'

import pgmagick as pg
import matplotlib.pyplot as plt
%matplotlib inline    #this line makes sure that image could be displayed on ipython notebook

def trans_mask_sobel(img):
    """Generate a transparency mask for a given image """

    image = pg.Image(img)

    # Find object
    image.negate()
    image.edge()
    image.blur(1)
    image.threshold(24)
    image.adaptiveThreshold(5, 5, 5)

    # Fill background
    image.fillColor('magenta')
    w, h = image.size().width(), image.size().height()
    image.floodFillColor('0x0', 'magenta')
    image.floodFillColor('0x0+%s+0' % (w-1), 'magenta')
    image.floodFillColor('0x0+0+%s' % (h-1), 'magenta')
    image.floodFillColor('0x0+%s+%s' % (w-1, h-1), 'magenta')

    image.transparent('magenta')
    return image


def alpha_composite(image, mask):
    """ Composite two images together by overriding one opacity channel """

    compos = pg.Image(mask)
    compos.composite(image, image.size(), pg.CompositeOperator.CopyOpacityCompositeOp)
    return compos


def remove_background(filename):
    """ Remove the background of the image in 'filename' """

    img = pg.Image(filename)
    transmask = trans_mask_sobel(img)
    img = alpha_composite(transmask, img)
    img.trim()
    img.write('out.jpg')


def show_image(filename):
    im = plt.imread(filename)
    plt.imshow(im)