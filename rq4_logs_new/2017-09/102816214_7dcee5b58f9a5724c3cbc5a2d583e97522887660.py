import cv2
import numpy as np
import pickle

cap = cv2.imread('target green.jpg')
win1 = 'Video 1'
win2 = 'Video 2'
win3 = 'Video 3'
win_y = 400
win_x = win_y*16//9
pos_xoffset = 62
l_thresh = [190, 255]

frame = cap

hls = cv2.cvtColor(frame, cv2.COLOR_BGR2HLS)
l = hls[:, :, 1]
l_selected = np.zeros_like(l)  # all pixels 0
l_selected[(l > l_thresh[0]) & (l <= l_thresh[1])] = 255  # selected pixels 1
cv2.namedWindow(win2, cv2.WINDOW_NORMAL)
cv2.resizeWindow(win2, win_x, win_y)
cv2.moveWindow(win2, x=win_x + pos_xoffset, y=0)
cv2.imshow(win2, l_selected)
cv2.waitKey()

img = np.copy(frame)
im2, contours, hierarchy = cv2.findContours(l_selected, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)

co = []
for c in contours:
    if len(c) > 100:
        co.append(c)

print(len(co), co)

cv2.drawContours(img, co, -1, (0, 0, 255), 3)
for i in range(len(co)):
    temp = np.copy(frame)
    cv2.drawContours(temp, co, i, (0, 0, 255), 3)
    cv2.namedWindow('1', cv2.WINDOW_NORMAL)
    cv2.imshow('1', temp)
    cv2.waitKey()


cv2.namedWindow(win1, cv2.WINDOW_NORMAL)
cv2.resizeWindow(win1, win_x, win_y)
cv2.moveWindow(win1, x=0, y=0)
cv2.imshow(win1, img)
cv2.waitKey()

# with open('contour.txt', 'a') as f:
#     f.write(contours)
