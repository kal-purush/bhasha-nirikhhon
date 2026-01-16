from tkinter import *


gui = Tk()
gui.title('PlaybyPlay')

gui.geometry('500x500+200+200')
Label(gui, text="Import your video here!").pack(pady=10)
text_box = Entry(gui, textvariable="text_box")
text_box.pack()


#time for buttons
def Retrieve():
    return None
Button(gui, text='Search', command=Retrieve).pack(pady=5)
gui.bind('<Return>', Retrieve)


gui.mainloop()