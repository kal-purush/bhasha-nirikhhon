# Visit Our facebook page: Uncle Engineer
# www.facebook.com/UncleEngineer
# www.uncle-engineer.com/python
# if error open cmd: pip install Pillow

from tkinter import *
from tkinter import ttk

from PIL import Image, ImageTk
import random
import time


    
def chinesedict():

    Voc = Toplevel()
    CN_EN = {'猫':'mao.jpg',
             '人':'ren.jpg',
             '工程师':'gongchengshi.png'}
    voc, pic = random.choice(list(CN_EN.items()))

    image = Image.open(pic)
    photo = ImageTk.PhotoImage(image)

    label = Label(Voc,image=photo)
    label.image = photo 
    label.pack()

    translate = Label(Voc, text = voc)
    translate.config(font=("FangSong", 44, 'bold'))
    translate.pack(ipady=20)
    Voc.mainloop()
        
if __name__=='__main__':
    
    GUI = Tk()
    GUI.title('Chinese Flash Card by Uncle Engieer')
    GUI.geometry("400x200+30+30") 


    chinese = Label(GUI, text = "Chinese Flash Card\n by Uncle Engineer")
    chinese.config(font=("tohama", 25))
    chinese.pack(padx=20,pady=20)

    B = ttk.Button(GUI, text ="Vocab", command = chinesedict)
    B.pack()

    GUI.mainloop()