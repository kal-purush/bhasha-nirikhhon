import re
import glob
from PIL import Image

NEW_DIM = (130, 100)
NEW_ZOOM = 500
DATA_PATH = './data/'
LOW_RM = DATA_PATH + 'LowRm/'
HIGH_RM = DATA_PATH + 'HighRm/'

TRANSFORMED = './transformed_zoom{}_dim{}x{}/'.format(NEW_ZOOM, NEW_DIM[0], NEW_DIM[1])


# TODO: It should clear data which does not have specified zoom in its name
def clear_data():
    pass


# TODO: normalize data with zoom, resolution etc etc
def normalize_data(data):
    out = []
    for d in data:
        p, im, res, zoom = d

        o_p = TRANSFORMED + p
        if zoom < NEW_ZOOM:
            centre = int(res[0] / 2), int(res[1] / 2)
            range_res = res[0] / (NEW_ZOOM / zoom), res[1] / (NEW_ZOOM / zoom)
            start_res = int(centre[0] - range_res[0] / 2), int(centre[1] - range_res[1] / 2)
            end_res = int(centre[0] + range_res[0] / 2), int(centre[1] + range_res[1] / 2)
            area = (start_res[0], start_res[1], end_res[0], end_res[1])
            o_im = im.crop(area)
            o_zoom = NEW_ZOOM
        else:
            o_im = im
            o_zoom = zoom

        o_im = o_im.resize(NEW_DIM)
        res = NEW_DIM

        out.append([o_p, o_im, res, o_zoom])
        # first zoom than resize

    return out


def augment_data():
    pass


def create_svm_classifier():
    pass


def create_knn_classifier():
    pass


def create_cnn_classfier():
    pass


def find_zoom(s):
    patter = '\d+x'
    r = re.findall(patter, s)
    return int(r[-1][:-1])


def file_with_resolution(s):
    im = Image.open(s)
    return im, im.size


def load_data():
    low = glob.glob(LOW_RM + '*.jpg')
    high = glob.glob(HIGH_RM + '*.jpg')

    low_with_zoom = [[x, file_with_resolution(x)[0], file_with_resolution(x)[1], find_zoom(x)] for x in low]
    high_with_zoom = [[x, file_with_resolution(x)[0], file_with_resolution(x)[1], find_zoom(x)] for x in high]

    return low_with_zoom, high_with_zoom


def find_bounds(l):
    zoom = (min(l, key=lambda x: x[3])[3], max(l, key=lambda x: x[3])[3])
    width = (min(l, key=lambda x: x[2][0])[2][0], max(l, key=lambda x: x[2][0])[2][0])
    height = (min(l, key=lambda x: x[2][1])[2][1], max(l, key=lambda x: x[2][1])[2][1])

    return {'category': ('min', 'max'), 'zoom': zoom, 'width': width, 'height': height}


if __name__ == '__main__':
    clear_data()
    low, high = load_data()
    print(low[0])
    print(find_bounds(low + high))
    normalized_low = normalize_data(low)
    normalized_high = normalize_data(high)

    from pathlib import Path
    Path(TRANSFORMED).mkdir(parents=True, exist_ok=True)
    Path(TRANSFORMED+LOW_RM).mkdir(parents=True, exist_ok=True)
    Path(TRANSFORMED+HIGH_RM).mkdir(parents=True, exist_ok=True)

    for o_p, o_im, res, o_zoom in normalized_low[2:6]:
        o_im.save(o_p, 'JPEG')