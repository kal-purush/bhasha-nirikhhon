import numpy as numpy
import cv2
import urllib

url = 'http://192.168.43.1:8080/shot.jpg'

    while True:
        img_resp = urllib.request.urlopen(url)
        img_np = np.array(bytearray(img_resp.read()),dtype=np.uint8)
        img_clr = cv2.imdecode(img_np,-1)
        img_gray = cv2.cvtColor(img_clr, cv2.COLOR_BGR2GRAY)

        v = np.median(img_gray)
        sigma = 0.6

        lower = int(max(0, (1.0 - sigma) * v))
        upper = int(min(255, (1.0 + sigma) * v))

        filter = cv2.Canny(img_gray,lower,upper)
    
        cv2.imshow('Original',img_clr)
        cv2.imshow('Canny Filter',filter)

        if ord('q')==cv2.waitKey(10):
            exit(0)



















