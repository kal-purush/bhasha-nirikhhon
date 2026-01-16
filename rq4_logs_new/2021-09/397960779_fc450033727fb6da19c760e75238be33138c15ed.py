from tkinter import *

from diary_tool import *

root = Tk()

# change root window h, w and border

root.geometry('1920x1080')
root.minsize('600', '400')
root.maxsize('1920', '1080')


# define functions for gui use
def test_func():
    myLabel = Label(root, text="hello")
    myLabel.pack()

def question_input():
    test_func()
    input_window = Tk()
    input_window.geometry('600x400+700+370')
    
    input_field = Entry(input_window)
    input_field.place(relx=0.5, rely=0.5)
    input_window.mainloop()


# setting up control buttons

ask_btn = Button(root, text="ASK MY QUESTIONS", width=22)

input_questions_btn = Button(root, text="INPUT QUESTIONS", width=22, command=question_input)
clear_questions_btn = Button(root, text="CLEAR QUESTIONS", width=22)
reset_log_btn = Button(root, text="RESET LOG", width=22)

# positioning buttons using rel which is relative to parent window

ask_btn.place(relx=0.5, rely=0.6, anchor=CENTER)
input_questions_btn.place(relx=0.5, rely=0.65, anchor=CENTER)
clear_questions_btn.place(relx=0.5, rely=0.7, anchor=CENTER)
reset_log_btn.place(relx=0.5, rely=0.75, anchor=CENTER)


# keep the window displaying
root.mainloop()
