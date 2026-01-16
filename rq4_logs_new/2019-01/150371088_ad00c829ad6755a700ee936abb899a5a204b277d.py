#coding=utf-8
#Version:python3.6.0
#Tools:Pycharm 2017.3.2
# Author:LIKUNHONG
__date__ = '2019/1/19 16:46'
__author__ = 'likunkun'

# 通过滑块来表示某个范围内的一个数字
from tkinter import *
root = Tk()
Scale(root, from_=0, to = 42).pack()
Scale(root, from_=0, to = 200, orient = HORIZONTAL).pack()

mainloop()

# 使用get（）方法可以获得当前滑块的位置
from tkinter import *
root = Tk()
s1 = Scale(root, from_=0, to = 42)
s1.pack()
s2 = Scale(root, from_=0, to = 200, orient = HORIZONTAL)
s2.pack()

def show():
    print(s1.get(),s2.get())

Button(root, text = '获取位置', command = show).pack()

mainloop()

# 通过resolution控制分辨率（步长），tickinterval设置刻度
from tkinter import *
root = Tk()
s1 = Scale(root, from_=0, to = 42, tickinterval = 5, length = 200, resolution = 5,orient = VERTICAL)
s1.pack()
s2 = Scale(root, from_=0, to = 200, tickinterval = 10, length = 600, orient = HORIZONTAL)
s2.pack()

mainloop()