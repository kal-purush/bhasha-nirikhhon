import os.path
import os
from collections import deque

import cv2
import numpy as np
import cupy as cp

#import warnings
#warnings.filterwarnings('ignore')


class ImageUtilities():

    WIDTH_DISPLAY = 640

    try:
        __USE_CUPY = os.environ['USE_CUPY']
        try:
            __USE_CUPY = int(__USE_CUPY)
        except ValueError:
            pass
        if __USE_CUPY: print('{} Use cupy'.format('*' * 8))
    except KeyError:
        __USE_CUPY = False

    try:
        __USE_OPENCV_DFT = os.environ['USE_OPENCV_DFT']
        try:
            __USE_OPENCV_DFT = int(__USE_OPENCV_DFT)
        except ValueError:
            pass
        if __USE_OPENCV_DFT: print('{} Use OpenCV DFT'.format('*' * 8))
    except KeyError:
        __USE_OPENCV_DFT = False

    def dft_cv(img):
        #f = cv2.dft(img)
        f = cv2.dft(img.astype(dtype=np.float32), flags=cv2.DFT_COMPLEX_OUTPUT)
        return f

    def idft_cv(img):
        f = cv2.idft(img)
        return f

    def fft_numpy(img):
        f = np.fft.fft2(img)
        return f

    def fft_cupy(img):
        f = cp.fft.fft2(cp.asarray(img))
        return f

    def ifft_numpy(img):
        f = np.fft.ifft2(img)
        return f

    def ifft_cupy(img):
        f = cp.fft.ifft2(cp.asarray(img))
        
    def numpy_cupy_conversion(img):
        a = cp.asarray(img)
        a = cp.asnumpy(a).astype(np.uint8)
        return a

    @classmethod
    def fftfilter(cls, gray, mask):
        assert (len(gray.shape) == 2 or gray.shape[-1]==1), \
            'Input image for FFT must be monochrome.'
        ny, nx = gray.shape
        if cls.__USE_OPENCV_DFT:
            f = cv2.dft(gray.astype(dtype=np.float32), flags=cv2.DFT_SCALE|cv2.DFT_COMPLEX_OUTPUT)
            f[:, :, 0] *= mask
            ret = cv2.idft(f, flags=cv2.DFT_REAL_OUTPUT)
            #ret = cv2.idft(f)
            #ret = cv2.magnitude(ret[:,:,0], ret[:,:,1])
        elif cls.__USE_CUPY:
            f = cp.fft.fft2(cp.asarray(gray))
            f = f * mask
            ret = cp.asnumpy(cp.fft.ifft2(f)).astype(np.uint8)
        else:
            f = np.fft.fft2(gray)
            f = f * mask
            ret = np.fft.ifft2(f).astype(np.uint8)
        ret = np.clip(ret, 0, 255).astype(np.uint8)
        return ret

    @classmethod
    def imwindow(cls, img, w_window, h_window=None):
        '''
        Down sample input image by cropping to central part of the image.
        '''
        h, w, *_ = img.shape
        if h_window is None:
            h_window = int(h * w_window / w + 0.5)
        y = int((h - h_window) / 2 + 0.5)
        x = int((w - w_window) / 2 + 0.5)
        return img[y:y+h_window, x:x+w_window]

    @classmethod
    def put_text_multi(cls, img, text, pos, *args, **kwargs):
        scale = args[1]
        x, y = pos
        y = int(y * scale)
        line_height = int(24 * scale)
        for line in text.split('\n'):
            line = line.strip()
            cv2.putText(img, line, (x, y), *args, **kwargs)
            y += line_height
            
    @classmethod
    def imshow(cls, window_name, img, overlay=None):
        '''
        Down sample input image, maintain original aspect ratio, and
        display in OpenCV window.
        '''
        h, w, *_ = img.shape
        h *= cls.WIDTH_DISPLAY / w
        w = cls.WIDTH_DISPLAY
        h = int(h + 0.5)

        #display = cv2.resize(img, dsize=(w, h), interpolation=cv2.INTER_CUBIC)
        display = cls.imwindow(img, cls.WIDTH_DISPLAY)

        if overlay:
            cls.put_text_multi(display, overlay, (8, 32), cv2.FONT_HERSHEY_SIMPLEX,
                0.6, (0, 255, 0), 1, cv2.LINE_AA)

        cv2.imshow(window_name, display)
        return display

    def round_powof_two(v):
        return 2 ** int(np.log(v) / np.log(2) + 0.5)

    @classmethod
    def get_freq_mask(cls, shape, range_=(0, 0.97)):
        height, width = shape
        low, high = range_
        black = 0
        white = 1
        mask = np.full((height, width), black, dtype=np.uint8)
        if high < 1:
            axes = np.array((width*high/2, height*high/2), dtype=int)
            center = np.array((width/2, height/2), dtype=int)
            box = np.hstack((center - axes, center + axes)).tolist()
            poly = cv2.ellipse2Poly(tuple(center.tolist()), tuple(axes.tolist()), 0, 0, 360, 6)
            cv2.fillConvexPoly(mask, poly, (white,))
        if low > 0:
            axes = np.array((width*low/2, height*low/2), dtype=int)
            center = np.array((width/2, height/2), dtype=int)
            box = np.hstack((center - axes, center + axes)).tolist()
            poly = cv2.ellipse2Poly(tuple(center.tolist()), tuple(axes.tolist()), 0, 0, 360, 6)
            cv2.fillConvexPoly(mask, poly, (black,))
        #mask = cv2.GaussianBlur(mask, (9, 9), 0)
        mask = np.fft.fftshift(mask)
        return mask


def normalize(a, min_, max_, type_=np.float):
    normalized = ((a - np.min(a)) * (max_ - min_) / np.ptp(a) + min_)
    return normalized.astype(type_)

def fftest():
    filename = os.path.join('..', 'images', 'sample_01.jpg')
    img = cv2.imread(filename)
    img = cv2.resize(img, dsize=(1280, 720), interpolation=cv2.INTER_CUBIC)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    cv2.imshow('Image', gray)
    cv2.waitKey(0)
    print(type(gray), gray.shape, gray.dtype)
    fft = np.fft.fft2(gray)
    #fft = cv2.dft(gray)

    mask = ImageUtilities.get_freq_mask(fft.shape)
    cv2.imshow('Mask', np.fft.fftshift(mask * 255))
    cv2.imwrite(os.path.join('..', 'images', 'sample_01_mask.jpg'), np.fft.fftshift(mask))
    cv2.waitKey(0)

    #fshift = np.fft.fftshift(fft)
    fft = fft * mask
    #fft = np.fft.ifftshift(fshift)

    #gray = np.real(np.fft.ifft2(fft))
    #fshift = np.fft.fftshift(fft)
    #f_ishift = np.fft.ifftshift(fshift)
    d_shift = np.array(np.dstack([fft.real, fft.imag]))
    gray = cv2.idft(d_shift, flags=cv2.DFT_SCALE | cv2.DFT_REAL_OUTPUT)
    gray = np.clip(gray, 0, 255)
    #gray = gray[:, :, 0]
    print(gray.shape)
    #gray = cv2.magnitude(gray[:,:,0], gray[:,:,1])
    #print(gray.shape)
    cv2.imshow('Filtered', gray.astype(np.uint8))
    cv2.imwrite(os.path.join('..', 'images', 'sample_01_filtered.jpg'), gray)
    cv2.waitKey(0)


class ImagePyramid():

    def __init__(self, img):
        self.__levels = deque()
        level = 0
        self.__levels.appendleft((level, img))
        h, w = img.shape
        while w > 256:
            level += 1
            img = cv2.pyrDown(img)
            self.__levels.appendleft((level, img))
            h, w = img.shape
        print('{} levels of image pyramid is constructed'.format(len(self.__levels)))

    @property
    def images(self):
        return self.__levels


if __name__ == '__main__':
    #fftest()
    print(5000, ImageUtilities.round_powof_two(2500))
    print(3000, ImageUtilities.round_powof_two(1500))

    filename = os.path.join('..', 'images', 'sample_01.jpg')
    img = cv2.imread(filename)
    img = cv2.resize(img, dsize=(1280, 720), interpolation=cv2.INTER_CUBIC)
    b, g, r = cv2.split(img)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    cv2.imshow('Image', img)
    cv2.waitKey(0)

    mask = ImageUtilities.get_freq_mask(gray.shape, range_=(0., 0.4))

    filtered = ImageUtilities.fftfilter(gray, mask)
    cv2.imshow('Filtered', filtered)
    cv2.imshow('Difference', cv2.merge(
        (b, np.clip(g+(np.abs(gray-filtered)/4), 0, 255).astype(np.uint8), r)))
    cv2.waitKey(0)