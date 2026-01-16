import tkinter
from tkinter import *
from tkinter import messagebox
root=tkinter.Tk()
root.title("Home Automation")
root.geometry("520x320")
root['bg']="white"
def hello():
    print("hello")
f1=Frame(root, height=80, width=320)
f1.pack( fill=X)
status="On"
l1=Message(f1,text=status,font=("aerial",10),bd=6)
l1.place(x=50,y=10)
root.resizable(0,0)
b1=Button(f1,text="Light",command=hello)
b1.place(x=150,y=10)
l1=Label(f1,text="Password")
l1.place(x=50,y=50)
currentpass="123456"
l1=Label(f1,text=currentpass)
l1.place(x=150,y=50)
f2=Frame(root, height=150, width=320)
f2.pack( fill=X)
lb1=Listbox(f2,relief="sunken", height=7, width=20)
lb1.place(x=50,y=10)
for i in range(1,5):
    lb1.insert(i,i)
f3=Frame(root, height=60, width=320)
f3.pack( fill=X)
b2=Button(f3,text="Vaccation Mode",command=hello)
b2.place(x=50,y=20)
b3=Button(f3,text="Close",command=root.destroy)
b3.place(x=180,y=20)
root.mainloop()