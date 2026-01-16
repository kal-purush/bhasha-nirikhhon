
import cv2
from pylab import *
import numpy as np
from skimage import color

from scipy.interpolate import interp1d, splprep, splev

class blush(object):

    def __init__(self,img):
        self.r = 0
        self.g = 0
        self.b = 0
        self.intensity = 0
        self.blush_radius = 1.02
        # Original image
        self.image = img
        # All the changes will be applied to im_copy
        self.im_copy = self.image.copy()
        self.height, self.width = self.image.shape[:2]


    def get_boudary(self,landmarks_x,landmarks_y):

        center_right_cheek = np.empty([2], dtype=int)
        cenetr_left_cheek = np.empty([2], dtype=int)
        #right cheek
        r_right_cheek = (landmarks_x[15] - landmarks_x[35]) / 3.5
        center_right_cheek[0] = (landmarks_x[15] + landmarks_x[35]) / 2.0
        center_right_cheek[1] = (landmarks_y[15] + landmarks_y[35]) / 2.0
        #left cheeck
        r_left_cheeck = (landmarks_x[1] - landmarks_x[31]) / 3.5
        cenetr_left_cheek[0] = (landmarks_x[1] + landmarks_x[31]) / 2.0
        cenetr_left_cheek[1] = (landmarks_y[1] + landmarks_y[31]) / 2.0

        return  r_right_cheek, center_right_cheek,r_left_cheeck,cenetr_left_cheek

    def fill(self,r,center):
        points_1 = [center[0] - r, center[1]]
        points_2 = [center[0], center[1] - r]
        points_3 = [center[0] + r, center[1]]
        points_4 = [center[0], center[1] + r]
        points_5 = points_1

        points = np.array([points_1, points_2, points_3, points_4, points_5])

        x, y = points[0:5, 0], points[0:5, 1]

        tck, u = splprep([x, y], s=0, per=1)
        unew = np.linspace(u.min(), u.max(), 1000)
        xnew, ynew = splev(unew, tck, der=0)
        tup = c_[xnew.astype(int), ynew.astype(int)].tolist()
        coord = list(set(tuple(map(tuple, tup))))
        coord = np.array([list(elem) for elem in coord])
        return np.array(coord[:, 0], dtype=np.int32), np.array(coord[:, 1], dtype=np.int32)


    def apply_blush(self,landmarks_x,landmarks_y,r,g,b,intensity):
        self.r = r
        self.g = g
        self.b = b
        self.intensity = intensity

        r_right_cheek, center_right_cheek,r_left_cheeck,cenetr_left_cheek = self.get_boudary(landmarks_x,landmarks_y)
        
        x_right, y_right = self.fill(r_right_cheek, center_right_cheek)
        x_left, y_left = self.fill(r_left_cheeck,cenetr_left_cheek)

        self.blush(x_right, y_right, x_left, y_left)


        return self.im_copy


    def blush(self, x_right, y_right, x_left, y_left):

        intensity = 0.8
        # Create blush shape
        mask = np.zeros((self.height, self.width))
        cv2.fillConvexPoly(mask, np.array(c_[x_right, y_right], dtype='int32'), 1)
        cv2.fillConvexPoly(mask, np.array(c_[x_left, y_left], dtype='int32'), 1)
        mask = cv2.GaussianBlur(mask, (51, 51), 0) * intensity
        print(np.array(c_[x_right, y_right])[:,0])
        val = cv2.cvtColor(self.im_copy, cv2.COLOR_RGB2LAB).astype(float)

        val[:, :, 0] = val[:, :, 0] / 255. * 100.
        val[:, :, 1] = val[:, :, 1] - 128.
        val[:, :, 2] = val[:, :, 2] - 128.

        LAB = color.rgb2lab(np.array((self.r / 255., self.g / 255., self.b / 255.)).reshape(1, 1, 3)).reshape(3,)
        mean_val = np.mean(np.mean(val, axis=0), axis = 0)

        mask = np.array([mask,mask,mask])
        mask = np.transpose(mask, (1,2,0))

        lab = np.multiply((LAB - mean_val), mask)

        val[:, :, 0] = np.clip(val[:, :, 0] + lab[:,:,0], 0, 100)
        val[:, :, 1] = np.clip(val[:, :, 1] + lab[:,:,1], -127, 128)
        val[:, :, 2] = np.clip(val[:, :, 2] + lab[:,:,2], -127, 128)

        self.im_copy = (color.lab2rgb(val) * 255).astype(np.uint8)
