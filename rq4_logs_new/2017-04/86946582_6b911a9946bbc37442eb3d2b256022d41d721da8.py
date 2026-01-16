import cv2

def crop(image, top=50, bottom=20, left=10, right=10):
    return image[top:-bottom, left:-right, :]

def resize(image, new_x=64, new_y=64):
    return cv2.resize(image, (new_x, new_y))
