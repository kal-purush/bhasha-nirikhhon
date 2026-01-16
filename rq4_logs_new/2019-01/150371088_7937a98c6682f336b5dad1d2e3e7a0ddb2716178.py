#coding=utf-8
#Version:python3.6.0
#Tools:Pycharm 2017.3.2
# Author:LIKUNHONG
__date__ = '2019/1/15 17:27'
__author__ = 'likunkun'

# 添加了label的Frame，可以更好地让checkbutton和radiobutton分组
from tkinter import *
root = Tk()
group = LabelFrame(root, text='全世界最好的语言是？',padx=5, pady=5)
group.pack(padx=10, pady=10)
contents = [
    ('Python',1),
    ('c',2),
    ('java',3),
    ('PHP',4)
]
v = IntVar()
v.set(1)
for lang ,num in contents:
    b = Radiobutton(group, text=lang, variable=v, value=num)
    b.pack(anchor=W)

mainloop()