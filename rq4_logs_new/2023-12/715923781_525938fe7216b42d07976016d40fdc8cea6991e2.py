from tkinter import *
import random
from datetime import datetime    

target = 1
play_time = 0
num_t = 0

def play():
    global target, play_time
    if target < num_t:
        target += 1
        btn2.destroy()
        ran_btn()
    else:
        fin = datetime.now()
        play_time = (fin - start).total_seconds()
        btn2.destroy()
        lab=Label(win)
        lab.config(text=f"Clear {play_time:.2f} 초. 표적 개수: {num_t}")
        lab.grid(column=0, row=0, padx=230, pady=155)
        restart_btn = Button(win, text="Restart", command=restart_game)
        restart_btn.grid(column=0, row=1)

def ran_btn():
    global btn2, target_image
    target_image = PhotoImage(file='image/target2.png')
    btn2 = Button(win, command=play, image=target_image, text=target, compound='center', highlightthickness=0, bd=0)
    btn2.place(relx=random.random(), rely=random.random())

def restart_game():
    for pl in win.place_slaves():
        pl.destroy()
    for pk in win.pack_slaves():
        pk.destroy()
    for gr in win.grid_slaves():
        gr.destroy()
    win.geometry("550x150")
    create_label()
    create_entry()
    create_button()
    
def create_label():
    global lab
    lab = Label(win, text="표적 개수")
    lab.grid(column=0, row=0, padx=20, pady=20)

def create_entry():
    global ent
    ent = Entry(win)
    ent.grid(column=1, row=0, padx=20, pady=20)
    
def create_button():
    global btn
    btn = Button(win, text="시작", command=start_game)
    btn.grid(column=0, row=1, columnspan=2)

def start_game():
    global num_t, start, target, play_time
    num_t = int(ent.get())
    target = 1
    play_time = 0
    for wg in win.grid_slaves():
        wg.destroy()
    
    win.geometry("800x450")
    ran_btn()
    start = datetime.now()

if __name__ == '__main__':
    win = Tk()
    win.title("AimingGame")
    win.geometry("550x150")
    win.option_add("*Font", "함초롱바탕 20")

    create_label()

    create_entry()

    create_button()

    win.mainloop()