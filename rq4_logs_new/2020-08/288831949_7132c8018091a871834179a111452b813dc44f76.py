import random
from tkinter import *
from PIL import ImageTk, Image
import random

# opens window
window = Tk()

# sets title of window
window.title('Dice Rolling Simulator')

# sets background to black
window.configure(background = 'black')

# text
Label (window, text = 'Welcome to', bg = 'black', fg = 'white', font = 'none 24 bold') .grid(row = 1, column = 1, sticky = '')
Label (window, text = 'Dice Rolling Simulator!',bg='black', fg='blue', font='none 32 bold') .grid(row=2,column=1, sticky='')

# original dice image 
dice1 = ImageTk.PhotoImage(Image.open('dice_1.png'))
Label (window, image=dice1, bg='black') .grid(row=4, column=1, sticky='')

# load all other dice images
dice2 = ImageTk.PhotoImage(Image.open('dice_2.png'))
dice3 = ImageTk.PhotoImage(Image.open('dice_3.png'))
dice4 = ImageTk.PhotoImage(Image.open('dice_4.png'))
dice5 = ImageTk.PhotoImage(Image.open('dice_5.png'))
dice6 = ImageTk.PhotoImage(Image.open('dice_6.png'))

# ROLL button
def roll():
	number=random.randint(1,6)
	Label (window, text='You rolled a...', bg='black', fg='white', font= 'none 18 bold') .grid(row=6, column=1, sticky='')
	Label (window, text=number, bg='black', fg='red', font='none 24 bold') .grid(row=7, column=1, sticky='')
	
	if number == 1:
		Label (window, image=dice1, bg='black') .grid(row=4, column=1, sticky='')

	if number == 2:	
		Label (window, image=dice2, bg='black') .grid(row=4, column=1, sticky='')
	
	if number == 3:
                Label (window, image=dice3, bg='black') .grid(row=4, column=1, sticky='')

	if number == 4:
                Label (window, image=dice4, bg='black') .grid(row=4, column=1, sticky='')

	if number == 5:
                Label (window, image=dice5, bg='black') .grid(row=4, column=1, sticky='')

	if number == 6:
                Label (window, image=dice6, bg='black') .grid(row=4, column=1, sticky='')
	
roll = Button(window, text='ROLL', command=roll, height=3, width=6, font='none 18 bold') .grid(row=8 , column=1, sticky=W)

# QUIT button
def quit():
	window.destroy()
quit = Button(window, text='QUIT', command=quit, height=3, width=6, font='none 18 bold') .grid(row=8, column=1, sticky=E)

# keeps window open
window.mainloop()