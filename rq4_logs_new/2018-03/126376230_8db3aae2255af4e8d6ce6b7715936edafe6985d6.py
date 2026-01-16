#
#count down timer 
#
# by Brian Corteil aka on Twitter @CannonFodder
#
# free to use
#

import time, datetime
from datetime import timedelta
import Tkinter
from time import *

# enter Event target time and date here
day= 12
month= 04
year= 2018
hour= 9
minutes= 00
sec= 0
targetTime = datetime.datetime(year, month, day, hour, minutes) # sets up target time

def update():
    timeNow =datetime.datetime.now() 
    remainingTime=(targetTime-timeNow)
    days = remainingTime.days
    if days >= 30:
        canvas.create_rectangle(150,340,660,460,fill='green')
    elif days < 30 and days >= 7:
        canvas.create_rectangle(150,340,660,460,fill='yellow')
    elif days < 7 and days >= 0:
        canvas.create_rectangle(150,340,660,460,fill='hot pink')
    else:
        canvas.create_rectangle(150,340,660,460,fill='red')
                                
    canvas.create_text(200, 360, anchor='center',text='Days', font=('Ariel','20'))
    canvas.create_text(400, 360, anchor='center',text='Hours',font=('Ariel','20'))
    canvas.create_text(600, 360, anchor='center',text='Minutes',font=('Ariel','20'))                            
                                
    canvas.create_text(200, 420, anchor='center',text=str(days),font=('Ariel','60'),fill='black')
    secs = remainingTime.seconds
    hrs, secs = divmod(secs, 3600)
    canvas.create_text(400, 420, anchor='center',text=str(hrs),font=('Ariel','60'),fill='black')
    mins, secs = divmod(secs, 60)
    canvas.create_text(600, 420, anchor='center',text=str(mins),font=('Ariel','60'),fill='black')
    root.after(1000,update)
                                
root=Tkinter.Tk()  
root.title('Upcoming Event Countdown Timer')
canvas = Tkinter.Canvas(root, height=480, width=800)
canvas.grid(row =10, column =3, sticky='w')
photo = Tkinter.PhotoImage(file = './dualsport.gif')
# resize image
photo = photo.zoom(3)
photo = photo.subsample(2)
canvas.create_image(0, 0, anchor='nw',image=photo)
canvas.create_rectangle(0,0,800,65,fill='blue')
# enter Event info here ...
canvas.create_text(400,20,anchor='center',text='MSTA Staunton Spring Romp',font=('Helvetica','20'),fill='white')
canvas.create_text(400, 50, anchor='center',text='April 12-15, 2018', font=('Helvetica','20'),fill='yellow')

root.after(1000,update)
root.mainloop()
  
exit()