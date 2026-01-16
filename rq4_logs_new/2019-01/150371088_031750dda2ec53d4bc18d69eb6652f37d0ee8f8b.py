#coding=utf-8
#Version:python3.6.0
#Tools:Pycharm 2017.3.2
# Author:LIKUNHONG
__date__ = '2019/1/19 15:33'
__author__ = 'likunkun'

# 滚动条，必须要和其他组件配合起来使用
'''如果要在某个组件上安装滚动条（垂直），需要做：
（1）设置该组件的yscrollbarcommand选项为Scrollbar组件的set()方法
（2）设置Scrollbar组件的command选项为该组件的yview（）方法'''

from tkinter import *
root = Tk()
sb = Scrollbar(root)
sb.pack(side = RIGHT, fill = Y)
lb = Listbox(root, yscrollcommand = sb.set)

for i in range(1000):
    lb.insert(END, str(i))

lb.pack(side = LEFT, fill = BOTH)
sb.config(command = lb.yview)

mainloop()