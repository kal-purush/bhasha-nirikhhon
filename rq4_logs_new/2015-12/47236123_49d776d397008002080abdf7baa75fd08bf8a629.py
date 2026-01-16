from Tkinter import *

master = Tk()
master.geometry("1024x800")

global textoy, h
textox = 128
textoy = 43


def arriba(event):
    global textoy, textox, h
    if textoy > 43:
        textoy = int(float(textoy) - 43)
    w.coords(j, textox, textoy)
    w.delete(h)
    h=w.create_text(950,750, text=str(textox)+","+str(textoy))
def abajo(event):
    global textoy, textox, h
    if textoy < 706:
        textoy = int(float(textoy) + 43)
    w.coords(j, textox, textoy)
    w.delete(h)
    h=w.create_text(950,750, text=str(textox)+","+str(textoy))
def izquierda(event):
    global textoy, textox, h
    if textox > 47:
        textox = int(float(textox) - 47)
    w.coords(j, textox, textoy)
    w.delete(h)
    h=w.create_text(950,750, text=str(textox)+","+str(textoy))
def derecha(event):
    global textoy, textox, h
    if textox < 849:
        textox = int(float(textox) + 47)
    w.coords(j, textox, textoy)
    w.delete(h)
    h=w.create_text(950,750, text=str(textox)+","+str(textoy))


w = Canvas(master, height=800, width=1024)
w.pack()


img1 = PhotoImage(file="mapa.gif")
f = w.create_image(0,0,anchor=NW,image=img1)
h = w.create_text(950,750, text=str(textox)+","+str(textoy))

j = w.create_text(textox,textoy,text="X", font='Helvetica 16 bold', fill="red")
master.bind('<Left>', izquierda)
master.bind('<Right>', derecha)
master.bind('<Up>', arriba)
master.bind('<Down>', abajo)




mainloop()