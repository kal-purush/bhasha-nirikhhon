#package_get.py
# 动态识别二维码，并打印结果
import sys
import numpy as np
import cv2
#import RPi.GPIO as GPIO
import time
#GPIO.setmode(GPIO.BCM)
#GPIO.setup(17, GPIO.OUT) #锁1

cap = cv2.VideoCapture(0)  # 设备号为0
cap.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc('M', 'J', 'P', 'G'))
cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
cap.set(cv2.CAP_PROP_FPS, 10)
def compare(data):
    filenew = open(r"user.txt", "w", encoding="utf8")
    filenew.write(data)
    file = open(r"user1.txt", "r", encoding="utf8")
    filenew = open(r"user.txt", "r", encoding="utf8")
    message=file.read()  # 将识别到的信息存入
    messagenew=filenew.read()
    if messagenew==message
        GPIO.output(17, GPIO.HIGH)
        time.sleep(0.1)
        GPIO.output(17, GPIO.LOW)
        GPIO.cleanup(17)

while (True):
    if cap.isOpened() == False:
        print('can not open camera')
        break
    else:
        ret, frame = cap.read()  # 读取图像
        inputImage = frame

        #使用函数cv2.QRCodeDetector().detectAndDecode（）获取二维码的内容data并截取展示二维码的部分rectifiedImage
        data, bbox, rectifiedImage = cv2.QRCodeDetector().detectAndDecode(inputImage)
        if len(data) > 0:
            print("Decoded Data : {}".format(data))
            compare(data)     #将读入的信息存入txt中
            rectifiedImage = np.uint8(rectifiedImage)
            cv2.namedWindow("Results", cv2.WINDOW_AUTOSIZE)
            rectifiedImage = cv2.resize(rectifiedImage, (300, 300))
            cv2.imshow("Results", rectifiedImage)
        else:
            print("QR Code not detected")
            cv2.namedWindow('Rectified QRCode')
            cv2.imshow("Rectified QRCode", inputImage)

        mykey = cv2.waitKey(1)
        if mykey & 0xFF == ord('q'):
            #释放内存、关闭窗口、退出进程
            cap.release()
            cv2.destroyAllWindows()
            sys.exit(0)

    if ret == False:  # 图像读取失败则直接进入下一次循环
        continue