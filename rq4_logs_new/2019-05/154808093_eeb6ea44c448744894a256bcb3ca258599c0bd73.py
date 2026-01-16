import glob
import re
import os
import sys
from PIL import Image
import time
import numpy as np
import io
from serial_tool import GSerial

def run_time(func):
    def call_fun(*args, **kwargs):
        start_time = time.time()
        func(*args, **kwargs)
        end_time = time.time()
        print('run time：{}s'.format(end_time - start_time))
    return call_fun

class Light():
    def __init__(self):
        self.ser = None
        self.baud = 9600
        self.dev = None
        self.get_dev()

    def get_dev(self):
        ttyUSBs = glob.glob('/dev/ttyUSB*')
        ttyUSBs = ['/dev/ttyUSB1']
        #查找camera
        for dev in ttyUSBs:
            with GSerial(dev, self.baud, 'N', 1, 8) as ser:
                ser.write(b'\xff\x26\x00\x00\xee')
                ret = ser.read(4, 0.5)
                ser.write(b'\xff\x26\x00\x00\xee')
                ret = ser.read(4, 0.5)
                print(ret)
                self.dev = dev
                if len(ret) == 4:
                    if ret[0] == b'\xff' and ret[-1] == b'\xee':
                        self.dev = dev
                        break
        if self.dev:
            self.ser = GSerial(self.dev, self.baud, 'N', 1, 8)

    def close(self):
        self.ser.close()

    def light(self, action):
        if self.ser:
            if action == 'on':
                self.ser.write(b'\xff\x01\x01\x02\xee')
            else:
                self.ser.write(b'\xff\x01\x00\x01\xee')

class Camera():
    def __init__(self):
        self.ser = None
        self.baud = 460800
        self.dev = None
        self.get_dev()

    def get_dev(self):
        ttyUSBs = glob.glob('/dev/ttyUSB*')
        ttyUSBs = ['/dev/ttyUSB0']
        #查找camera
        for dev in ttyUSBs:
            with GSerial(dev, self.baud, 'N', 1, 8) as ser:
                ser.write(b'\x56\x00\x11\x00')
                ret = ser.read(0.05)
                if len(ret)>0:
                    self.dev = dev
                    print(ret)
                    break
        if self.dev:
            self.ser = GSerial(self.dev, self.baud, 'N', 1, 8)
            print('open camera success')

    def open(self):
        self.ser = GSerial(self.dev, self.baud, 'N', 1, 8)
        if self.ser:
            print('open success')

    def close(self):
        self.ser.close()

    def reboot(self):
        self.ser.write(b'\x56\x00\x26\x00')
        ret = self.ser.read(3)
        return ret

    def snap(self):
        self.ser.write(b'\x56\x00\x36\x01\x00')
        ret = self.ser.read(1, 5)
        if len(ret) == 0:
            print('snap fail')
        return ret

    #@run_time
    def get_picture(self):
        self.ser.write(b'\x56\x00\x34\x01\x00')
        ret = self.ser.read(1, 9)
        if len(ret) == 9:
            pic_len = ret[-4]<<24|ret[-3]<<16|ret[-2]<<8|ret[-1]
            print(f'get picture len: {ret}, {pic_len}')
        else:
            print('get picture length fail')
            return None

        time.sleep(0.5)
        bgn = time.time()
        self.ser.write(b'\x56\x00\x32\x0c\x00\x0a\x00\x00\x00\x00'+ret[-4:]+b'\x00\xff')
        ret = self.ser.read(pic_len, 20)
        end = time.time()
        print(f'get picture time: {end-bgn}s')
        #print(f'picturelen: {len(ret)}')
        #print(ret[0:20])
        #print(ret[-20:])
        #return ret[5:-5]
        img = Image.open(io.BytesIO(ret[5:-5]))
        return np.array(img)
        #img.show()
        ##return img

    def set_camera(self, size, baud=0):
        if size == 0:  # 320*240
            f = b'\x04'
            e = b'\x11'
        elif size == 1:# 640*480
            f = b'\x04'
            e = b'\x00'
        elif size == 2:# 160*120
            f = b'\x04'
            e = b'\x22'
        elif size == 3:# 1024*768
            f = b'\x05'
            e = b'\x33'
        elif size == 4:# 1280*720
            f = b'\x05'
            e = b'\x44'
        elif size == 5:# 1280*960
            f = b'\x05'
            e = b'\x55'
        else:
            e = b'\x00'
        self.ser.write(b'\x56\x00\x31\x05'+f+b'\x01\x00\x19'+e)
        ret = self.ser.read(5, 1)
        if len(ret)==0:
            print('set picture size fail')

        if baud == 0 or self.baud == baud:
            return
        if baud == 115200:
            e = b'\x0d\xa6'
        elif baud == 230400:
            e = b'\xee\xa1'
        elif baud == 460800:
            e = b'\xee\xa2'
        else:
            e = b'\x0d\xa6'
        self.ser.write(b'\x56\x00\x31\x06\x04\x02\x00\x08'+e)
        ret = self.ser.read(0.5)
        if len(ret) == 5:
            self.baud = baud
            print(ret)
            print(f'set baud {baud} success')

    def set_led(self, action):
        if action == 'on':
            self.ser.write(b'\x56\x00\x85\x01\x01')
        else:
            self.ser.write(b'\x56\x00\x85\x01\x00')
        ret = self.ser.read(0.5)
        if len(ret) == 0:
            print('set led fail')

if __name__ == '__main__':
    camera = Camera()
    #camera.open()
    #camera.reboot()
    #time.sleep(3)
    #camera.set_camera(3, 460800)
    #time.sleep(1)
    camera.set_led('on')
    while True:
        path = 'image/'
        str = input('> ')
        if str == 'q':
            break
        elif str == 'chess':
            path = 'chess/chess_'
            continue
        elif str == 'image':
            path = 'image/'
            continue
        camera.snap()
        time.sleep(0.5)
        img = camera.get_picture()
        if img:
            img.save(f'{path}{str}.jpg')
            
    camera.set_led('off')
