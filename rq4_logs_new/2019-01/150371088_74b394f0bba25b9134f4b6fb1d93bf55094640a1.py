#coding=utf-8
#Version:python3.6.0
#Tools:Pycharm 2017.3.2
# Author:LIKUNHONG
__date__ = '2019/1/12 10:26'
__author__ = 'likunkun'


'''图片在文字旁边'''
from tkinter import *
root = Tk()
# justify对齐方式，padx与边界距离
textLabel = Label(root, text = '你好呀！！！！！！！！！！\n！！！！！！！！！！！！！', justify=LEFT, padx=10)
textLabel.pack(side=LEFT)
# 创建一个Label对象
# 用PhotoImage实例化一个图片对象
photo = PhotoImage(file='photo.jpg')
imgLabel = Label(root, image=photo)
imgLabel.pack(side=RIGHT)

mainloop()


'''图片在文字后边（背景）'''
from tkinter import *
root = Tk()
photo = PhotoImage(file='photo.jpg')
imgLabel = Label(root, image=photo)
tehLabel = Label(root,
                  text = '你好呀！！！！！',
                  justify=CENTER,   # 字居中对齐
                  image=photo,
                  compound=CENTER,   # 设置图像和文本重叠的模式
                  font=("微软雅黑", 20),
                  fg='black'    # 字体颜色
                  )
tehLabel.pack()

mainloop()