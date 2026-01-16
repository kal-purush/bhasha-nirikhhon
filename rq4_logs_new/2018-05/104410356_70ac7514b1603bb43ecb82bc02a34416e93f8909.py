import scipy.misc
# from scipy import misc
import numpy as np
from numpy import shape
import matplotlib
# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import glob
import os
import pickle
import time

CURR_DIR = os.path.abspath(os.path.dirname(__file__)) + '/'
SEGMENTED_DIR = CURR_DIR + 'color-segmented-images/'
DATA_DIR = os.path.abspath(os.path.join(os.path.join(CURR_DIR, '..'),  '..')) + '/data/'
OUTDIR = CURR_DIR + 'pixel-vision-tiles'
if not os.path.isdir(OUTDIR):
    os.makedirs(OUTDIR)


def vision_pixtile_dir(img_id, k=500):
    # TODO: import from utils.py instead of redefining here
    vdir = '{}/{}/{}'.format(OUTDIR, k, img_id)
    if not os.path.isdir(vdir):
        os.makedirs(vdir)
    return vdir


def get_size(fname):
    from PIL import Image
    #Open image for computing width and height of image
    im = Image.open(fname)
    width = im.size[0]
    height = im.size[1]
    return width, height


def get_img_name_to_id():
    import csv
    img_name_to_id = {}
    with open(DATA_DIR + 'image.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            img_name_to_id[row['filename']] = row['id']
    return img_name_to_id


def rescale_factor(original_img_path, new_img_path):
    original_width, original_height = get_size(original_img_path)
    new_width, new_height = get_size(new_img_path)
    return float(original_width) / float(new_width)


def get_tiles_from_img(img):
    # NOTE: this just segments by color
    # disjointed same color pieces will be part of the same tile
    # assumption is that vision preprocessing has already separated stuff into different colors

    # TODO: use PixelEM_ADS::create_tiles_from_given_wmap()
    unique_vals = set()
    # for x, y in zip(range(shape(img)[0]), range(shape(img)[1])):
        # print x, y
    for x in range(shape(img)[0]):
        for y in range(shape(img)[1]):
            r, g, b = img[x, y, 0], img[x, y, 1], img[x, y, 2]
            # print r, g, b
            if tuple([r, g, b]) not in unique_vals:
                unique_vals.add(tuple([r, g, b]))

    unique_vals = list(unique_vals)
    print 'Num unique colors = ', len(unique_vals)

    tiles = []
    for i in range(len(unique_vals)):
        r, g, b = unique_vals[i]
        idx = np.where((img[:, :, 0] == r) & (img[:, :, 1] == g) & (img[:, :, 2] == b))
        tiles.append(zip(*idx))
        #print "Check all PixTile unique:", len(np.unique(img[idx[0],idx[1]]))==3
        # print i, len(tiles[-1])

    np.random.shuffle(tiles)

    print 'Num tiles = ', len(tiles)

    mask = np.zeros(shape(img)[:2])
    for tidx in range(len(tiles)):
        for i in list(tiles[tidx]):
            mask[i] = tidx

    return mask, tiles


def test_tiling_on_raw_img(k=500):
    # NOTE: this might give disjointed same color pieces into a common tile
    img = scipy.misc.imread(SEGMENTED_DIR + str(k) + '/COCO_train2014_000000000127.png')
    mask, tiles = get_tiles_from_img(img)
    plt.figure()
    plt.imshow(mask)
    plt.colorbar()
    # plt.show()
    plt.savefig(CURR_DIR + 'test_tiling_on_raw.png')
    plt.close()


def generate_all():
    img_name_to_id = get_img_name_to_id()
    print "Number of imgs to loop through:",len(glob.glob(SEGMENTED_DIR + '*/*.png'))
    for img_file in glob.glob(SEGMENTED_DIR + '*/*.png'):
	start = time.time()
        # if 'COCO_train2014_000000000127' not in img_file:
        #     continue
        img_name = img_file.split('/')[-1].split('.png')[0]
        k = img_file.split('/')[-2]
        # print img_name, k
        img = scipy.misc.imread(img_file)
        # print shape(img)
        original_img_path = '{}/{}.png'.format(CURR_DIR + 'COCO', img_name)
        img = scipy.misc.imresize(img, rescale_factor(original_img_path, img_file))
        # print shape(img)

        print '-------------------------------------------------------'
        print 'Generating pixtiles for ', img_file
        # print shape(img)

        mask, tiles = get_tiles_from_img(img)

        img_id = img_name_to_id[img_name]

        outdir = vision_pixtile_dir(img_id, k)

        with open('{}/{}.pkl'.format(outdir, 'pixtile_mask'), 'w') as fp:
            fp.write(pickle.dumps(mask))

        with open('{}/{}.pkl'.format(outdir, 'pixtile_list'), 'w') as fp:
            fp.write(pickle.dumps(tiles))

        plt.figure()
        plt.imshow(mask)
        plt.colorbar()
        # plt.show()
        plt.savefig('{}/{}.png'.format(outdir, 'viz_mask'))
        plt.close()
	end = time.time()
	print "Time Elapsed:",end-start
if __name__ == '__main__':
    # test_tiling_on_raw_img()
    generate_all()