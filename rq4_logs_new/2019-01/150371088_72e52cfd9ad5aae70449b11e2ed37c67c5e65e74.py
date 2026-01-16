#coding=utf-8
#Version:python3.6.0
#Tools:Pycharm 2017.3.2
# Author:LIKUNHONG
__date__ = '2019/1/20 13:34'
__author__ = 'likunkun'


import pygame
import sys

pygame.init()   # 初始化pygame
size = width, height = 600 ,400
speed = [-2,1]
bg = (255,255,255)
screen = pygame.display.set_mode(size)  # 创建制定大小的窗口
pygame.display.set_caption('初次见面，多多关照') # 设置窗口标题
photo = pygame.image.load('photo.jpg')
position = photo.get_rect()     # 获得图像的位置矩形

while True:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            sys.exit()
    position = position.move(speed)     # 移动图像
    if position.left < 0 or position.right > width:
        photo = pygame.transform.flip(photo, True, False)   # 翻转图像
        speed[0] = -speed[0]    # 反方向移动
    if position.top < 0 or position.bottom > height:
        speed[1] = -speed[1]
    screen.fill(bg)     # 填充背景
    screen.blit(photo, position)    # 更新图像
    pygame.display.flip()   # 更新界面
    pygame.time.delay(10)   # 延迟10毫秒