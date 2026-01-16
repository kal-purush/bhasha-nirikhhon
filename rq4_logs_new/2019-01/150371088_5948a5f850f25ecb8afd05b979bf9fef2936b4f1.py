# 普通显示一个对话框
'''
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
#coding=utf-8
#Version:python3.6.0
#Tools:Pycharm 2017.3.2
# Author:LIKUNHONG
__date__ = '2019/1/19 13:40'
__author__ = 'likunkun'

# Entyr可以验证输入内容的合法性
# 通过设置validate，validatecommand，invalidcommand三个选项实现
# validate：
#   focus：当组件获得或失去焦点的时候验证
#   focusin：当组件获得焦点的时候验证
#   focusout：当组件失去焦点时候验证
#   key：当输入框被编辑的时候验证
#   all：当出现上面任一种情况的时候验证
#   none：关闭验证
#
# validatecommand选项指定一个验证函数，该验证函数只能返回true或者false，表示验证的结果
# invalidcommand在上面的函数false后调用
'''
from tkinter import *
root = Tk()

def test():
    if e1.get() == 'lkk':
        return True
    else:
        e1.delete(0,END)
        return False

def test2():
    print('test2调用')
    return True

v = StringVar()
e1 = Entry(root, textvariable = v, validate = 'focusout', validatecommand=test, invalidcommand=test2)
e2 = Entry(root)
e1.pack(padx = 10, pady = 10)
e2.pack(padx = 10 ,pady = 10)

mainloop()
'''

from tkinter import *
root = Tk()
v = StringVar()

def test(content, reason, name):
    if content == 'lkk':
        print(content, reason, name)
        return True
    else:
        print(content, reason, name)
        return False

testCMD = root.register(test)   # 调用register吧验证函数包装起来
e1 = Entry(root, textvariable = v, validate = 'focusout', validatecommand = (testCMD, '%P', '%v', '%W'))
e2 = Entry(root)
e1.pack()
e2.pack()

mainloop()