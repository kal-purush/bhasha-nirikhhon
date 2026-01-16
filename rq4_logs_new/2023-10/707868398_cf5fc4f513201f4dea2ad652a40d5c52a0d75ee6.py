from tkinter import *
from tkinter.ttk import *
from tkinter.filedialog import askopenfile
import time

window=Tk()
window.title("Email Sending Automation")
window.geometry('500x500')

def open_file():
    file_path=askopenfile("r",filetypes=[('Image Files', '*jpeg'),('Image Files', '*jpg'),('Image Files', '*png')])
    if file_path is not None:
        pass



window.mainloop()