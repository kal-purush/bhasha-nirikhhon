#!/usr/bin/python
# -*- coding: UTF-8 -*-

from tkinter import *
# from os import walk

from tkinter import ttk
# from tkinter.filedialog import askdirectory
from tkinter.filedialog import askopenfilename
from tkinter.filedialog import askdirectory
import codepdf as co
import chainlp as ch
# import threading
from tkinter.messagebox import showinfo
import bartool


# 选择路径回调函数
def selectPath():
    path_ = askopenfilename()
    path.set(path_)


root = Tk()
path = StringVar()
root.title("OCR工具(支持pdf,png,jpg)_v1.2")
root.geometry('400x260+10+10')
root.resizable(0, 0)  # 防止用户调整尺寸


def reply():
    showinfo(title='消息', message='处理完毕')


# def fun_timer():
#     print('Hello Timer!')
#     # Label(root, text=co.doc_current).grid(row=1, column=0)
#     showinfo(title='消息', message=str(co.doc_current))
#     global timer
#     timer = threading.Timer(5.5, fun_timer)
#     timer.start()
#
# timer = threading.Timer(1, fun_timer)

def callBack(p, checkbox1, checkbox2):
    # timer.start()
    # Label(root, text=co.doc_current).grid(row=1, column=0)
    if None == p or len(p) <= 0:
        showinfo(title='消息', message='请选择路径！')
        return
    rw = 'w+'
    if checkbox1 == 1:
        rw = 'a+'
    bartool.execure()
    if checkbox2 == 2:
        # 执行图片拆分
        co.get_file(p)
        co.conver_img_only()
    elif p[p.rfind('.') + 1:len(p)] != 'pdf':
        co.ocrcat(p)
    else:
        co.get_file(p)
        # print(rw)
        co.conver_img(rw)
    bartool.quit()
    print('处理完毕')
    reply()


Label(root, text="目标文件路径:").grid(row=1, column=0)
Entry(root, textvariable=path, width=30).grid(row=1, column=1)
Button(root, text="路径选择", command=selectPath).grid(row=1, column=2)

CheckVar1 = IntVar()
# Checkbutton(root, text="生成单个文本", variable=CheckVar1, onvalue=1, offvalue=0).grid(row=2, column=0)
Radiobutton(root, text='生成多个文本', variable=CheckVar1, value=0).grid(row=2, column=0)
Radiobutton(root, text='生成单个文本', variable=CheckVar1, value=1).grid(row=2, column=1, sticky=W)
Radiobutton(root, text='pdf转图片', variable=CheckVar1, value=2).grid(row=2, column=1, sticky=E)
# CheckVar2 = IntVar()
# Checkbutton(root, text="pdf转图片", variable=CheckVar2, onvalue=2, offvalue=0).grid(row=2, column=1)

Label(root, text='请选择可读取的pdf或图片', justify=LEFT, compound='left').grid(row=0, column=0, columnspan=3, sticky=W)
Button(root, text="开始识别", fg="blue",
       command=lambda: callBack(p=path.get(), checkbox1=CheckVar1.get(), checkbox2=CheckVar1.get())).grid(row=2,
                                                                                                          column=2)

# 水平分割线
sh = ttk.Separator(root, orient=HORIZONTAL)
sh.grid(row=3, column=0, columnspan=3, sticky="we")
# 垂直分割线
# sv = ttk.Separator(root, orient=VERTICAL)
# sv.grid(row=1, column=2, rowspan=3, sticky="ns")
Label(root, text='请选择图片目录，合并为pdf').grid(row=4, column=0, columnspan=3, sticky=W)

# 选择路径回调函数
path2 = StringVar()


def selectPath2():
    path2_ = askdirectory()
    path2.set(path2_)


# 传值变量 (图片压缩后大小)
textVar = IntVar()
textVar.set(150)
e1 = Entry(root, textvariable=textVar, width=5)
e2 = Label(root, text='图片大小(kb)')


# 点击消失或显示
def printselection(p):
    if p % 2 == 1:
        e1.grid(row=6, column=1, padx=3)
        e2.grid(row=6, column=1, sticky=W)
    else:
        e1.grid_forget()
        e2.grid_forget()


CheckVar3 = IntVar()
Checkbutton(root, text="图片是否压缩", variable=CheckVar3, onvalue=1, offvalue=0,
            command=lambda: printselection(p=CheckVar3.get())).grid(row=6, column=0)


def callBack2(p, usezip, zipkb):
    if None == p or len(p) <= 0:
        showinfo(title='消息', message='请选择路径！')
        return
    result_set = ch.getFolderName(p)
    result_list = list(result_set)
    result_list.sort()
    # print(result_list)
    bartool.execure()
    for dir in result_list:
        ch.pic2pdf(dir, usezip, zipkb)
    bartool.quit()
    print('处理完毕')
    reply()


Label(root, text="目标文件夹或目录:").grid(row=5, column=0)
Entry(root, textvariable=path2, width=30).grid(row=5, column=1)
Button(root, text="路径选择", command=selectPath2).grid(row=5, column=2)

Button(root, text="开始转换", fg="blue",
       command=lambda: callBack2(p=path2.get(), usezip=CheckVar3.get(), zipkb=textVar.get())).grid(row=6, column=2)
#######
# 水平分割线
sh = ttk.Separator(root, orient=HORIZONTAL)
sh.grid(row=7, column=0, columnspan=3, sticky="we")
Label(root, text='请选择doc文件或docx文件', justify=LEFT).grid(row=8, column=0, columnspan=3, sticky=W)

# 选择路径回调函数
path3 = StringVar()


def selectPath3():
    path3_ = askopenfilename()
    path3.set(path3_)


def callBack3(p):
    # doc_files = []
    # directory = "C:\\Users\\xkw\\Desktop\\destData"
    # for root, dirs, filenames in walk(directory):
    #     for file in filenames:
    if None == p or len(p) <= 0:
        showinfo(title='消息', message='请选择路径！')
        return
    if p.endswith(".doc") or p.endswith(".docx"):
        bartool.execure()
        ch.doc2pdf(p, p[0:p.rfind('.')])
        bartool.quit()
    else:
        showinfo(title='消息', message='请选择正确的文件格式！')
    print('处理完毕')
    reply()


Label(root, text="目标文件路径:").grid(row=9, column=0)
Entry(root, textvariable=path3, width=30).grid(row=9, column=1)
Button(root, text="路径选择", command=selectPath3).grid(row=9, column=2)

Button(root, text="开始转换", fg="blue", command=lambda: callBack3(p=path3.get())).grid(row=10, column=2)

# root.rowconfigure(1,weight=1)

# def refresh_data(root):
#     w = Label(root, text=str(co.doc_current)+"/"+str(co.doc_count)).grid(row=1, column=0)
#     root.after(10000, refresh_data(root))
# refresh_data(root)
root.mainloop()