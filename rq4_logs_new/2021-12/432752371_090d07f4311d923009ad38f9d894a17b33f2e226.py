import tkinter as tk
from tkinter import *
from CustomButton import CustomButton

def changing_information(button_list):
    for x in button_list:
        x.get_button().config(
            background=CustomButton.color,
            activebackground=CustomButton.activebackground,
        )
        x.get_frame().place(
            x=x.calculate_x_coordinate(),
            width=CustomButton.width,
            height=CustomButton.height,
        )


class App:
    def __init__(self, master):
        self.__master = master
        self.__master.title("Project")
        self.__master.geometry("700x700")
        self.__master.config(
            background="#A49D8A", highlightthickness="10", highlightcolor="grey"
        )

    def run(self):
        self.__master.mainloop()


master = tk.Tk()
button_list = []
button_list.append(
    CustomButton(
        master,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        button_command=lambda: changing_information(button_list),
    )
)
button_list.append(CustomButton(master, "aaa", button_command=None))
button_list.append(CustomButton(master, "aaa", button_command=None))
button_list.append(CustomButton(master, "aaa", button_command=None))
button_list.append(CustomButton(master, "aaa", button_command=None))

app = App(master)
app.run()