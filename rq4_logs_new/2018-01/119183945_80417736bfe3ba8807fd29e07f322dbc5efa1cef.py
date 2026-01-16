from scipy.ndimage.measurements import center_of_mass
from time import time
import numpy as np
import cv2

class Face():
    def __init__(self, shape):
        self.shape = shape
        M = cv2.moments(self.shape)
        cx = int(M["m10"] / M["m00"])
        cy = int(M["m01"] / M["m00"])
        self.center = cx, cy

class Powercube():
    TOP_BOUNDS = (20, 100, 180), (50, 200, 255)
    SIDE_BOUNDS = (20, 180, 100), (50, 255, 255)
    BOTTOM_BOUNDS = (15, 240, 100), (20, 255, 180)
    
    def __init__(self):
        self.mask = None
        self.top = None
        self.hull = None

class CubeNotFoundException(BaseException):
    pass

def center(mask):
    return tuple(np.mean(np.where(mask), axis=1).astype(np.int0))[::-1]

def centroid(points):
    M = cv2.moments(points)
    cx = int(M["m10"] / M["m00"])
    cy = int(M["m01"] / M["m00"])
    return cx, cy

def find_face(image, lowerb, upperb):
    face = cv2.inRange(image, lowerb, upperb)
    blurred = cv2.blur(face, (10, 10))
    opened = cv2.morphologyEx(blurred, cv2.MORPH_OPEN, np.ones((7, 7), np.uint8))
    ret, thresh = cv2.threshold(opened, 50, 255, cv2.THRESH_BINARY)
    return thresh

def find_cube(image):
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    top_face = find_face(hsv, *Powercube.TOP_BOUNDS)
    side_faces = find_face(hsv, *Powercube.SIDE_BOUNDS)
    bottom_face = find_face(hsv, *Powercube.BOTTOM_BOUNDS)
    full = top_face | side_faces | bottom_face

    if len(np.where(full)[0]) < 100:
        raise CubeNotFoundException()

    cube = Powercube()
    cube.mask = full
    ret, contours, hierarchy = cv2.findContours(top_face, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    contour = max(contours, key=cv2.contourArea)
    hull = cv2.convexHull(contour)
    approx = cv2.approxPolyDP(hull, 0.05 * cv2.arcLength(hull, True), True)
    cube.top = Face(approx)
    
    ret, contours, hierarchy = cv2.findContours(full, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    contour = max(contours, key=cv2.contourArea)
    hull = cv2.convexHull(contour)
    cube.hull = hull
    
    return cube

def find_scale(image):
    hsv = cv2.cvtColor(image, cv2.COLOR_BGR2HSV)
    top_line = cv2.inRange(hsv, (90, 70, 0), (120, 150, 255))
    bottom_line = cv2.inRange(hsv, (100, 140, 0), (130, 255, 255))
    intersect = lambda shift: np.roll(top_line, shift, axis=0) & bottom_line

    shift = 5
    intersection = np.roll(top_line, shift, axis=0) & bottom_line
    new = intersection
    i = intersection.sum()
    n = i + 1
    while n > i:
        intersection = new
        i = n
        shift += 1
        new = np.roll(top_line, shift, axis=0) & bottom_line
        n = new.sum()

    size = shift - 1
    ret, mask = cv2.threshold(intersection, 50, 255, cv2.THRESH_BINARY)

    line_top = np.hstack((np.ones((size, size)), -np.ones((size, size))))
    line_kernel = np.vstack((line_top, line_top[:, ::-1])) / (2 * size ** 2)
    down = cv2.filter2D(mask, -1, line_kernel)
    up = cv2.filter2D(mask, -1, line_kernel[::-1])
    ret, cross = cv2.threshold(down + up, 50, 255, cv2.THRESH_BINARY)
    
    opened = cv2.morphologyEx(cross, cv2.MORPH_CLOSE, np.ones((size, size), np.uint8))
    ret, thresh = cv2.threshold(opened, 50, 255, cv2.THRESH_BINARY)
    
    ret, contours, hierarchy = cv2.findContours(thresh, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    *areas, = map(cv2.contourArea, contours)
    areamax = np.argmax(areas)
    largest = contours[areamax]
    crosses = np.array(contours)[np.array(areas) > 0.75 * areas[areamax]]

    centroids = []
    for x in crosses:
        hull = cv2.convexHull(x)
        cv2.drawContours(image, [hull], -1, (0, 255, 0), 3)
        centroids.append(centroid(hull))

    return centroids