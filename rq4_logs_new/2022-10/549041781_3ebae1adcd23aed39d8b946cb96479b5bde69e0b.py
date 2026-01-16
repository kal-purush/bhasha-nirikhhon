from tkinter import *
from tkinter import messagebox
import pyqrcode

ws = Tk()
ws.title("QR GENERATOR")
ws.config(bg='#ff99dd')

def generate_QR():
    if len(user_input.get())!=0 :
        global qr,img
        qr = pyqrcode.create(user_input.get())
        img = BitmapImage(data = qr.xbm(scale=6))
    else:
        messagebox.showwarning('warning', 'All Fields are Required!')
    try:
        display_code()
    except:
        pass

def display_code():
    img_lbl.config(image = img)
    output.config(text="Generated QR code of  " + user_input.get())


lbl = Label(
    ws,
    text="Give Your Message:",
    bg='#ff99dd'
    )
lbl.pack()

user_input = StringVar()
entry = Entry(
    ws,
    textvariable = user_input
    )
entry.pack(padx=10)


button = Button(
    ws,
    text = "Generate",
    width=15,
    command = generate_QR
    )
button.pack(pady=10)

img_lbl = Label(
    ws,
    bg='#ff99dd')
img_lbl.pack()
output = Label(
    ws,
    text="Thank You For Using Me",
    bg='#ff99dd'
    )
output.pack()
 
ws.mainloop()