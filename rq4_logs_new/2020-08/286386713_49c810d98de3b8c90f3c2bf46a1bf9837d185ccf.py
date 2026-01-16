# -*- coding: UTF-8 -*-

'''
pip install pillow
'''
from PIL import Image
import os

#获取图片文件的大小
def get_size(file):
    # 获取文件大小:KB
    size = os.path.getsize(file)
    return size / 1024

#拼接输出文件地址
def get_outfile(infile, outfile):
    if outfile:
        return outfile
    dir, suffix = os.path.splitext(infile)
    outfile = '{}-out{}'.format(dir, suffix)
    filetup = os.path.split(infile)
    outfile = filetup[0] + '/out/' + filetup[1]
    return outfile

#转移文件地址
def move_fileto_out(infile, outfile):
    if outfile:
        return outfile
    dir, suffix = os.path.splitext(infile)
    outfile = '{}-out{}'.format(dir, suffix)
    return outfile

#压缩文件到指定大小
def compress_image(infile, outfile='', mb=150, step=10, quality=80):
    """不改变图片尺寸压缩到指定大小
    :param infile: 压缩源文件
    :param outfile: 压缩文件保存地址
    :param mb: 压缩目标，KB
    :param step: 每次调整的压缩比率
    :param quality: 初始压缩比率
    :return: 压缩文件地址，压缩文件大小
    """
    # print('infile', infile,get_size(infile))
    outfile = get_outfile(infile, outfile)
    o_size = get_size(infile)
    if o_size <= mb:
        im = Image.open(infile)
        im.save(outfile)
        return outfile, get_size(outfile)
    while o_size > mb:
        im = Image.open(infile)
        im.save(outfile, quality=quality)
        if quality - step < 0:
            break
        quality -= step
        o_size = get_size(outfile)
    return outfile, get_size(outfile)

#转移文件
def compress_image2(infile, outfile='', mb=150, step=10, quality=80):
    outfile = move_fileto_out(infile, outfile)
    o_size = get_size(infile)
    if o_size <= mb:
        im = Image.open(infile)
        im.save(outfile)
        return outfile, get_size(outfile)
    while o_size > mb:
        im = Image.open(infile)
        im.save(outfile, quality=quality)
        if quality - step < 0:
            break
        quality -= step
        o_size = get_size(outfile)
    os.remove(infile)
    return outfile, get_size(outfile)

#修改图片尺寸，如果同时有修改尺寸和大小的需要，可以先修改尺寸，再压缩大小

def resize_image(infile, outfile='', x_s=1376):
    """修改图片尺寸
    :param infile: 图片源文件
    :param outfile: 重设尺寸文件保存地址
    :param x_s: 设置的宽度
    :return:
    """
    im = Image.open(infile)
    x, y = im.size
    y_s = int(y * x_s / x)
    out = im.resize((x_s, y_s), Image.ANTIALIAS)
    outfile = get_outfile(infile, outfile)
    out.save(outfile)




"""
    先来说一下jpg图片和png图片的区别
    jpg格式:是有损图片压缩类型,可用最少的磁盘空间得到较好的图像质量
    png格式:不是压缩性,能保存透明等图
    
    pip install opencv-python
    pip install opencv-contrib-python
    https://ziyubiti.github.io/2016/06/15/imreaderror/
    pip install --upgrade opencv-python
    
*将image = cv2.imread(image_path)替换为：image = cv2.imdecode(np.fromfile(image_path,dtype=np.uint8),-1)即可。
同样，如果要保存图像为中文文件名，则将cv2.imwrite(output_image_path, image)替换为cv2.imencode('.jpg', image)[1].tofile(output_image_path)即可。
"""
from PIL import Image
import cv2 as cv
import numpy as np
import os

def PNG_JPG(PngPath):
    img = cv.imdecode(np.fromfile(PngPath,dtype=np.uint8),-1)
    wh = img.shape[::-1]
    infile = PngPath
    outfile = os.path.splitext(infile)[0] + ".jpg"
    img = Image.open(infile)
    img = img.resize((int(wh[0] / 2), int(wh[1] / 2)), Image.ANTIALIAS)
    try:
        if len(img.split()) == 4:
            # prevent IOError: cannot write mode RGBA as BMP
            r, g, b, a = img.split()
            img = Image.merge("RGB", (r, g, b))
            img.convert('RGB').save(outfile, quality=70)
            os.remove(PngPath)
        else:
            img.convert('RGB').save(outfile, quality=70)
            os.remove(PngPath)
        return outfile
    except Exception as e:
        print("PNG转换JPG 错误", e)

if __name__ == '__main__':
    # compress_image(r'C:\Users\Administrator\Pictures\席文楷图片\马廷军.jpg')
    # resize_image(r'D:\learn\space.jpg')
    PNG_JPG(r"C:\Users\Administrator\Desktop\快捷方式\电子书\《杨显惠命运三部曲（套装共3本）（夹边沟记事_定西孤儿院_甘南纪事）》 - 杨显惠\newfold\out\《杨显惠命运三部曲（套装共3本）（夹边沟记事_定西孤儿院_甘南纪事）》 - 杨显惠0-out.png")