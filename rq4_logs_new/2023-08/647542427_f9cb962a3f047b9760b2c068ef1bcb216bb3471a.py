# day 28 - more tkinter, dynamic typing
# project - pomodoro app

from tkinter import *
import math

# essentials
FONT = 'Courier'
MAIN_BGCOLOR = '#FFBA86'
FG_COLOR = '#557A46'
CHECK = '✔'
WORK_MIN = 25
BREAK_MIN = 5
LONG_BREAK = 20
reps = 0
timer = None

# work with canvas widget and add images
# UI setup
window = Tk()
window.title('Pomodoro')
# setting up window padding, background color
window.config(padx=100, pady = 50, bg=MAIN_BGCOLOR)
# timer functionality
def sec_countdown(num):
    # display purposes
    current_min = math.floor(num / 60)
    current_sec = num % 60
    if current_sec < 10:
        current_sec = f"0{current_sec}"
    if current_min < 10:
        current_min = f"0{current_min}"
    canvas.itemconfig(ctd_timer, text = f"{current_min}:{current_sec}")

    if num > 0:
        global timer
        timer = window.after(1000, sec_countdown, num-1)
        start_btn['state'] = 'disabled'
    else:
        start_timer()
        check_badge = ''
        work_sessions = math.floor(reps/2)
        for _ in range(work_sessions):
            check_badge += CHECK
        check_lbl.config(text=check_badge)

# start button
def start_timer():
    global reps
    reps += 1
    
    if reps % 8 == 0:
        sec_countdown(LONG_BREAK * 60)
        timer_lbl.config(text='Break')
    elif reps % 2 == 0:
        sec_countdown(BREAK_MIN * 60)
        timer_lbl.config(text='Break')
    else:
        sec_countdown(WORK_MIN * 60)
        timer_lbl.config(text='Work')
    


# reset button
def reset_timer():
    global reps
    reps = 0
    window.after_cancel(timer)
    timer_lbl.config(text= 'Timer')
    canvas.itemconfig(ctd_timer, text = '25:00')
    start_btn['state'] = 'active'

# setting up canvas size, background color
canvas = Canvas(width=200, height=224, bg=MAIN_BGCOLOR, highlightthickness=0)
# getting image
tomato_img = PhotoImage(file='tomato.png')
# setting image position
canvas.create_image(100, 112, image = tomato_img)
# setting timer label
ctd_timer = canvas.create_text(100, 130, text = '25:00', fill = 'white', font=(FONT, 30, 'bold'))

# challenge - complete UI
# timer label
timer_lbl = Label(text='Timer', font=(FONT, 32, 'bold'), bg=MAIN_BGCOLOR, fg=FG_COLOR)
# start button
start_btn = Button(
    text='Start', 
    command = start_timer, 
    bg=MAIN_BGCOLOR, 
    relief=GROOVE, 
    padx=10, pady=10,
    )
# reset button
reset_btn = Button(
    text='Reset', 
    command = reset_timer, 
    bg=MAIN_BGCOLOR, 
    relief=GROOVE, 
    padx=10, pady=10,
    )
# checkmark
check_lbl = Label(text='', font=(FONT, 12, 'bold'), bg=MAIN_BGCOLOR)

# countdown mechanism


# pack everything
canvas.grid(column=1, row=1)
timer_lbl.grid(column=1, row=0)
start_btn.grid(column=0, row=2)
reset_btn.grid(column=2, row=2)
check_lbl.grid(column=1, row=3)

window.mainloop()
