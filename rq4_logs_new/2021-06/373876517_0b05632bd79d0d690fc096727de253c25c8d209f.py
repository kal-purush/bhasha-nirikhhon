from tkinter import *
from tkinter.ttk import *
from time import strftime
root=Tk()
root.title("Digital Clock")
label=Label(root,font=("arial",15,"bold","underline"),text="Digital Clock",foreground="black")
label.pack()
def time():
    string=strftime('%H:%M:%S %p')
    label.config(text=string)
    label.after(1000,time)
label=Label(root,font=("ds-digital",60),background="black",foreground="red")
label.pack()
button=Button(root,text="Close",width=15,command=root.destroy)
button.pack()
time()
mainloop()