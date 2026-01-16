#coding=utf-8
#Version:python3.6.0
#Tools:Pycharm 2017.3.2
# Author:LIKUNHONG
__date__ = '2019/1/17 20:01'
__author__ = 'likunkun'

# 输入框，

# 普通显示一个对话框
'''
from tkinter import *

root = Tk()
e = Entry(root)
e.pack(padx=20, pady=20)
e.delete(0, END)
e.insert(0, '默认文本')

mainloop()
'''

# 添加一个按钮，单击按钮，获得输入框的内容打印出来，然后清空输入框
from tkinter import *
root = Tk()
# Tkinter总共提供了三种布局组件的方法：pack,gird,place
# grid方法允许你用表格的形式来管理组件，row行，column列
Label(root, text='账号：').grid(row=0)
Label(root, text='密码：').grid(row=1)
v1 = StringVar()
v2 = StringVar()
e1 = Entry(root, textvariable = v1)
e2 = Entry(root, textvariable = v2, show='*')
e1.grid(row=0, column = 1, padx = 10, pady = 5)
e2.grid(row=1, column = 1, padx = 10, pady = 5)

def show():
    print("账号：%s" % e1.get())
    print("密码：%s" % e2.get())
    e1.delete(0, END)
    e2.delete(0, END)

# 如果表格大于组件，那么可以使用sticky选项来设置组件的位置
# 用NESW以及他们的组合NE，SE，SW，NW来表示方位
Button(root, text='获取信息', width=10, command = show) \
    .grid(row=3, column=0, sticky=W, padx=10, pady=5)
Button(root, text='退出', width=10, command = root.quit)\
    .grid(row=3, column=1, sticky = E, padx=10, pady = 5)

mainloop()