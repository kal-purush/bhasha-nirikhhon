import subprocess
import cv2
from matplotlib import pyplot as plt
import numpy as np
import math
import time
import random


class Jumper:
    # pos[0] is horizontal, pos[1] is vertical
    pos = (0, 0)
    target_pos = (0, 0)
    jump_coeff = 1.392

    def __init__(self):
        pass

    # 此函数用于对手机屏幕截屏
    @staticmethod
    def senseTheWorld():
        cmd = 'adb shell screencap -p'
        completed_process = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
        screenshot = completed_process.stdout
        # the following line cannot be omitted
        binary_screenshot = screenshot.replace(b'\r\n', b'\n')
        f = open('world.png', 'wb')
        f.write(binary_screenshot)
        f.close()

    # 此函数用于寻找棋子底座圆心的像素坐标， 利用了opencv中的模板匹配算法
    def findSelfPosition(self):
        img = cv2.imread('world.png', 0)
        template = cv2.imread('piece_head.png', 0)
        res = cv2.matchTemplate(img, template, cv2.TM_CCOEFF_NORMED)
        min_val, max_val, min_loc, max_loc = cv2.minMaxLoc(res)
        top_left = max_loc
        w, h = template.shape
        # TODO: replace the number 166 with the ratio between the height of the piece and the height of the screen
        self.pos = (int(top_left[0] + w/2), int(top_left[1] + h/2)+166)

    # 此函数用于寻找下一跳目标点的像素坐标，利用了中心对称来缩小搜索区域，同时利用了颜色以及跳台的特殊方位
    def findTargetPosition(self):
        img = cv2.imread('world.png')
        w = 1080
        h = 1920
        searchcenter = (w - self.pos[0], h - self.pos[1])
        x_start = searchcenter[0]-100
        y_start = searchcenter[1]-100
        cnt = 0
        sumpos = 0
        posx = 0
        posy = 0
        for i in range(y_start, searchcenter[1]):
            # cv2中图像的索引是反的
            reference_pixel = img[i, x_start].astype(np.int16)
            for j in range(x_start, searchcenter[0]+200):
                pixel = img[i, j].astype(np.int16)
                difference = abs(pixel[0] - reference_pixel[0]) + abs(pixel[1] - reference_pixel[1]) + abs(pixel[2] - reference_pixel[2])
                if difference > 20:
                    cnt += 1
                    sumpos += j
            if sumpos != 0:
                posx = int(sumpos/cnt)
                y_start = i
                break

        reference_pixel = img[y_start, posx].astype(np.int16)
        for k in range(y_start+280, y_start, -1):
            pixel = img[k, posx].astype(np.int16)
            difference = abs(pixel[0] - reference_pixel[0]) + abs(pixel[1] - reference_pixel[1]) + \
                abs(pixel[2] - reference_pixel[2])
            if difference < 10:
                posy = int((k + y_start)/2)
                break

        self.target_pos = (posx, posy)

    # 此函数用于使棋子跳跃
    def jump(self):
        distance2 = (self.pos[0] - self.target_pos[0])**2 + (self.pos[1] - self.target_pos[1])**2
        distance = math.sqrt(distance2)
        duration = int(self.jump_coeff * distance)
        print(duration)
        cmd = 'adb shell input swipe {x1} {y1} {x2} {y2} {duration}'.format(
            x1=540,
            y1=1800,
            x2=600,
            y2=1850,
            duration=duration
        )
        subprocess.run(cmd, shell=True)


if __name__ == '__main__':
    jumper = Jumper()
    while True:
        jumper.senseTheWorld()
        jumper.findSelfPosition()
        jumper.findTargetPosition()
        jumper.jump()
        time.sleep(random.uniform(1.5, 2.0))