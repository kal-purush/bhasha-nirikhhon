from tkinter import *
import random

fontsize = 30
colors = ['red', 'green', 'orange', 'cyan', 'purple']


def onSpam():
    # 建立一个顶级窗口
    popup = Toplevel()
    # y颜色随机选择color列表中的一种
    color = random.choice(colors)
    # 创建标题，并设计一系列的设置
    Label(popup, text='Popup', bg='black', fg=color).pack(fill=BOTH)
    mainLabel.config(fg=color)


def onFlip():
    # 自循环调用自己，随机改变字体颜色
    mainLabel.config(fg=random.choice(colors))
    main.after(250, onFlip)


def onGroW():
    global fontsize
    fontsize += 5
    mainLabel.config(font=('arial', fontsize, 'italic'))
    main.after(100, onGroW)


main = Tk()
mainLabel = Label(main, text='Fun Gui!', relief=RAISED)
mainLabel.config(font=('arial', fontsize, 'italic'), fg='cyan', bg='navy')
mainLabel.pack(side=TOP, expand=YES, fil=BOTH)
Button(main, text='spam', command=onSpam).pack(fill=X)
Button(main, text='flip', command=onFlip).pack(fill=X)
Button(main, text='grow', command=onGroW).pack(fill=X)
main.mainloop()