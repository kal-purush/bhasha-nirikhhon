import cv2 as cv
import matplotlib.pyplot as plt
import numpy as np
from screeninfo import get_monitors


CAM_DEVICE = 0
WIN_NAME = 'projector'
MON_NUM = 1
RESOLUTION = (1920,1080)  # WxH - 1080p

HUE_SHIFT = 5


cap = cv.VideoCapture(CAM_DEVICE)
# cap.open(0, apiPreference=cv.CAP_V4L2)
cap.set(cv.CAP_PROP_FRAME_WIDTH,RESOLUTION[0])
cap.set(cv.CAP_PROP_FRAME_HEIGHT,RESOLUTION[1])


cv.namedWindow(WIN_NAME)

cv.resizeWindow(WIN_NAME, get_monitors()[MON_NUM].width, get_monitors()[MON_NUM].height)
# cv.moveWindow(WIN_NAME, get_monitors()[MON_NUM].x, get_monitors()[MON_NUM].y)
cv.moveWindow(WIN_NAME, -RESOLUTION[0], 0)
cv.setWindowProperty(WIN_NAME, cv.WND_PROP_FULLSCREEN, cv.WINDOW_FULLSCREEN)


assert cap.isOpened()


while True:
    # Capture frame-by-frame
    ret, frame_in = cap.read()

    # if frame is read correctly ret is True
    if not ret:
        print("Can't receive frame (stream end?). Exiting ...")
        break


    hsv = cv.cvtColor(frame_in, cv.COLOR_BGR2HSV)
    h,s,v = cv.split(hsv)
    h = (h.astype(np.int16) + HUE_SHIFT) % 180
    h = h.astype(np.uint8)
    s = (s * 1.0).astype(np.uint8)
    v = (v * .5).astype(np.uint8)
    hsv = cv.merge([h, s, v])
    frame_out = cv.cvtColor(hsv, cv.COLOR_HSV2RGB)

    # Display the resulting frame
    cv.imshow(WIN_NAME, frame_out)


    if cv.waitKey(1) == ord('q'):
        break

# When everything done, release the capture
cap.release()
cv.destroyAllWindows()

j=1


