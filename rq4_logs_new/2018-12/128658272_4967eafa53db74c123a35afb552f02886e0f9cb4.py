# -*- coding: utf-8 -*-
"""
Date calculator.
for calculating periods between two dates.
Author Susan Bruce.
version 5.6  4/12/2018
"""
# datetime
import datetime
from datetime import timedelta
# tkinter
from tkinter import *
from tkinter import messagebox
### create instance of a window.
window_1 = Tk()  # window_1.
window_1.clipboard_clear()
cliptext = ""
#### static properties.
window_1.wm_iconbitmap('beatnik.ico')  # set window_1 icon
window_1.title("Calculate repeating date cycle")  # set window_1 title
window_1.geometry("430x325")  # set window_1 geometry
## test number of days input from entry widget to be in range for month days.
month_days = {0:31, 1:28, 2:31, 3:30, 4:31, 5:30, 6:31, 7:31, 8:30, 9:31, 10:30, 11:31}
def Verify_Month_Days_1(month_key, test_days):
    if test_days > (month_days[month_key]) or test_days < 1:
        messagebox.showinfo("Error first date", "number of days out of range for the selected month")
        return 1
    else:
        day_1 = int(entry_1.get())
        return day_1
def Verify_Month_Days_2(month_key, test_days):
    if test_days > (month_days[month_key]) or test_days < 1:
        messagebox.showinfo("Error second date", "number of days out of range for the selected month")
        return 1
    else:
        day_2 = int(entry_2.get())
        return day_2

### event handlers.
def Click_Event():
    year_1 = Set_Year_1()
    year_2 = Set_Year_2()
    month_1 = (listbox_1.curselection()[0]) + 1  # passes integer  
    if month_1 < 1:
        month_1 = 1
    try:
        entry_1.get() == int(entry_1.get())
    except ValueError:        
        day_1 = 1
    else:
        day_1 = int(entry_1.get())
        day_1 = Verify_Month_Days_1(month_1-1, day_1)
    month_2 = (listbox_2.curselection()[0]) + 1  # passes integer
    if month_2 < 1:
        month_2 = 1
    try:
        entry_2.get() == int(entry_2.get())
    except ValueError:        
        day_2 = 1
    else:
        day_2 = int(entry_2.get())
        day_2 = Verify_Month_Days_2(month_2-1, day_2)
    try:
        entry_3.get() == int(entry_3.get())
    except ValueError:        
        day_cycle = 1
    else:
        day_cycle = int(entry_3.get())
    if day_cycle > 365:
        day_cycle = 1
    d = datetime.date(year_1, month_1, day_1)
    print("date one ", d)
    cliptext = "date one " + str(d)+"\n"
    window_1.clipboard_append(cliptext)
    c = datetime.date(year_2, month_2, day_2)
    print("date two ", c)
    cliptext = "date two " + str(c)+"\n"
    window_1.clipboard_append(cliptext)
    x = c - d
    days_diff = x.days
    print("days cycle", day_cycle)
    cliptext = "days cycle " + str(day_cycle)+"\n"
    window_1.clipboard_append(cliptext)
    #print(type(days_diff))###<-toggle test type line(for variable)
    weeks_diff = days_diff // 7
    weeks_days_diff = (weeks_diff, (days_diff - (weeks_diff * 7)))
    print("number of days between date one and date two is", days_diff, "days")
    cliptext = "number of days between date one and date two is "+str(days_diff)+ " days\n"
    window_1.clipboard_append(cliptext)
    print((str(weeks_diff) + " Weeks and " + str(weeks_days_diff[1]) + " Days"))
    cliptext = str(weeks_diff) + " Weeks and " + str(weeks_days_diff[1]) + " Days\n\n"
    window_1.clipboard_append(cliptext)
    period = int(day_cycle)
    periods = days_diff // period

    print()
    print("the date's from ", d, "every", period, "days")
    cliptext = "the date's from "+str(d)+" every "+str(period)+" days\n"
    window_1.clipboard_append(cliptext)
#    date_format = '%d-%m-%Y'
    for delta in range(period, (periods*period+1), period):
        x = d + timedelta(delta)
#        p = x.strftime(date_format)
        q = x.strftime('%B %d %Y')
#        j =('%s-%s-%s' % (x.day, x.month, x.year))
        listbox_3.insert(END,q)
        print(q)
        cliptext = str(q)+"\n"
        window_1.clipboard_append(cliptext)
    cliptext = "\n"
    window_1.clipboard_append(cliptext)
    cliptest = window_1.clipboard_get()
def Click_Event_1():
    pass
    
# frame_0 and radio buttons and select year function
def Set_Year_1():  # return radio button selection
    year = v.get()
    return year
frame_0 = Frame(window_1)
frame_0.grid(row=0, column=0)
v = IntVar()
v.set(2018)  # initializing the choice
label_1 = Label(frame_0, text="First date   ")
label_1.grid(row=0, column=0)
r1a = Radiobutton(frame_0,
                 text="Last year",
                 variable=v,
                 command=Set_Year_1,
                 value=2017).grid(row=0, column=1)

r2a = Radiobutton(frame_0,
                 text="This year",
                 variable=v,
                 command=Set_Year_1,
                 value=2018).grid(row=0, column=2)        

r3a = Radiobutton(frame_0,
                 text="Next year",
                 variable=v,
                 command=Set_Year_1,
                 value=2019).grid(row=0, column=3)
# frame_1 and radio buttons and select year function
def Set_Year_2():  # return radio button selection
    year = z.get()
    return year
frame_1 = Frame(window_1)
frame_1.grid(row=1, column=0)
z = IntVar()
z.set(2018)  # initializing the choice
label_2 = Label(frame_1, text="Second date   ")
label_2.grid(row=0, column=0)
r1b = Radiobutton(frame_1,
                 text="Last year",
                 variable=z,
                 command=Set_Year_2,
                 value=2017).grid(row=0, column=1)

r2b = Radiobutton(frame_1,
                 text="This year",
                 variable=z,
                 command=Set_Year_2,
                 value=2018).grid(row=0, column=2)        

r3b = Radiobutton(frame_1,
                 text="Next year",
                 variable=z,
                 command=Set_Year_2,
                 value=2019).grid(row=0, column=3)
# frame_2 holds frames 3, 4, 5
frame_2 = Frame(window_1)
frame_2.grid(row=2, column=0)
### frame_3 widgets.
frame_3 = Frame(frame_2)
label_1a = Label(frame_3, relief = 'solid', width = 17)
label_1a.configure(text = "Start month of Period")
### listbow_1 and static attributes.
listbox_1 = Listbox(frame_3, exportselection=0, width = 12, height = 12)
for item in ['January','Febuary','March',
             'April','May','June',
             'July','August','September',
             'October','November','December']:
    listbox_1.insert(END,item)
listbox_1.selection_set(0)
label_2a = Label(frame_3, relief = 'groove', width = 10)
label_2a.configure(text = "Start day")
entry_1 = Entry(frame_3, width = 2)
entry_1.insert(0, "1")
### geometry frame_3
label_1a.grid (column = 0, row = 2)
frame_3.grid  (column = 0, row = 2)
listbox_1.grid(column = 0, row = 3)
label_2a.grid (column = 0, row = 5)
entry_1.grid  (column = 1, row = 5)
### frame_4 widgets.
frame_4 = Frame(frame_2)
label_1b = Label(frame_4, relief = 'solid', width = 16)
label_1b.configure(text = "End month of period")
### listbow_2 and static attributes.
listbox_2 = Listbox(frame_4, exportselection=0, width = 12, height = 12)
for item in ['January','Febuary','March',
             'April','May','June',
             'July','August','September',
             'October','November','December']:
    listbox_2.insert(END,item)
listbox_2.selection_set(0)
label_2b = Label(frame_4, relief = 'groove', width = 10)
label_2b.configure(text = "End day")
entry_2 = Entry(frame_4, width = 2)
entry_2.insert(0, "1")
### geometry frame_4
label_1b.grid (column = 3, row = 2)
frame_4.grid  (column = 1, row = 2)
listbox_2.grid(column = 3, row = 3)
label_2b.grid (column = 3, row = 5)
entry_2.grid  (column = 4, row = 5)
### frame_5 wigets
frame_5 = Frame(frame_2)
label_3a = Label(frame_5, relief = 'solid', width = 18)
label_3a.configure(text = "Calculated date's")
listbox_3 = Listbox(frame_5, width = 19, height = 12)
label_3b = Label(frame_5, relief = 'groove', width = 16)
label_3b.configure(text = "Num of days in cycle")
entry_3 = Entry(frame_5, width = 2)
entry_3.insert(0, "1")
### geometry frame_5
label_3a.grid (column = 4, row = 2)
frame_5.grid  (column = 2, row = 2)
listbox_3.grid(column = 4, row = 3)
label_3b.grid (column = 4, row = 5)
entry_3.grid  (column = 5, row = 5)
# frame_6 and complete calculation button
frame_6 = Frame(window_1)
calcBtn = Button(frame_6, width = 17, command = Click_Event)
calcBtn.configure(text = "Complete calculations")
spacer = Label(frame_6, width =2)
fileBtn = Button(frame_6, width = 22, command = Click_Event_1)
fileBtn.configure(text = "Write calculations to text file")
### geometry calc button
calcBtn.grid   (column = 0, row = 0)
spacer.grid    (column = 1, row = 0)
fileBtn.grid   (column = 2, row =0)
### geometry frame_6
frame_6.grid(column=0, row=9)
### sustain window.
window_1.mainloop()