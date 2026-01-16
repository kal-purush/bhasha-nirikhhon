from scipy.optimize import curve_fit
from PIL import Image
import numpy as np
import matplotlib.pyplot as plt
from skimage import img_as_bool, morphology


def gauss(x, mu, sigma, a):
    return a * np.exp(-(x - mu) ** 2 / (2 * sigma ** 2))


def bimodal2(x, *params):
    mu1, sigma1, a1, mu2, sigma2, a2 = params
    return gauss(x, mu1, sigma1, a1) + gauss(x, mu2, sigma2, a2)


def bimodal(x, mu1, sigma1, a1, mu2, sigma2, a2):
    return gauss(x, mu1, sigma1, a1) + gauss(x, mu2, sigma2, a2)


def main():
    with Image.open("3_2.tif") as img:
        img.load()
        y = np.array(img.histogram())
        x = np.arange(0, 256)

    expected2 = (200, 20, 1000)
    result = curve_fit(
        f=gauss,
        xdata=x,
        ydata=y,
        p0=expected2)

    #print(x)
    #print(len(result))
    params, cov = result
    #print(len(params))

    plt.plot(np.arange(0, 256), y, label='Test data')
    plt.plot(np.arange(0, 256), gauss(np.arange(0, 256), *params), label='Fitted data')
    plt.show()

    image_binary = img.point(lambda p: 0 if p > params[0]-0.5*params[1] else 255)
    img_binary = image_binary.convert("1")
    img_binary.show()
    no_holes = morphology.remove_small_holes(img_as_bool(img_binary), 15)
    skeleton = morphology.medial_axis(no_holes)
    #pruned_skeleton, segmented_img, segment_objects = pcv.morphology.prune(skel_img=skeleton, size=10)
    plt.imshow(skeleton, cmap='gray', interpolation='nearest')
    plt.plot(img_binary, skeleton)
    plt.show()


if __name__ == '__main__':
    main()