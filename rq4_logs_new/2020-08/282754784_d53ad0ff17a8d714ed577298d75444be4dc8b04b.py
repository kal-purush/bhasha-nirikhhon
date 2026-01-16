import State_driving
from tkinter import *
from pynput import keyboard
import threading
from ObserverAndMotorCommunication.ASDW_Control import MotorController

cont=MotorController()

asdw_w_wight = 300
asdw_w_height = 300


class ASDWDriving:
    def __init__(self):
        # if w or s and a or d are pressed, cannot be pressed one more time
        self.w_pressed = False
        self.s_pressed = False
        self.a_pressed = False
        self.d_pressed = False

        pass

    def asdw_window_f(self):
        self.asdw_window = Toplevel()
        self.asdw_window.geometry(str(asdw_w_wight) + "x" + str(asdw_w_height))
        self.asdw_w_c = Canvas(self.asdw_window)
        self.asdw_w_c.place(x=0, y=0)
        asdw_label = Label(self.asdw_w_c, text="You can use 'asdw' to drive \n and press 'q' to exit this mode")
        asdw_label.place(x=asdw_w_wight / 2, y=10, anchor=N)

        '''self.asdw_window.bind('<KeyPress-a>', lambda event, c="a": self.drive(event, c))
        self.asdw_window.bind('<KeyPress-d>', lambda event, c="d": self.drive(event, c))
        self.asdw_window.bind('<KeyPress-w>', lambda event, c="w": self.drive(event, c))
        self.asdw_window.bind('<KeyPress-s>', lambda event, c="s": self.drive(event, c))

        # self.asdw_window.bind('<KeyPress-q>', self.destroy_asdw_w)'''

        self.a_arrow = Label(self.asdw_w_c, fg="black", text=u"\u2190", font=("Ariel", 40))
        self.a_arrow.place(x=asdw_w_wight / 4, y=asdw_w_height / 2, anchor=W)

        self.d_arrow = Label(self.asdw_w_c, fg="black", text=u"\u2192", font=("Ariel", 40))
        self.d_arrow.place(x=asdw_w_wight * 3 / 4, y=asdw_w_height / 2, anchor=E)

        self.w_arrow = Label(self.asdw_w_c, fg="black", text=u"\u2191", font=("Ariel", 40))
        self.w_arrow.place(x=asdw_w_wight / 2, y=asdw_w_height / 2, anchor=S)

        self.s_arrow = Label(self.asdw_w_c, fg="black", text=u"\u2193", font=("Ariel", 40))
        self.s_arrow.place(x=asdw_w_wight / 2, y=asdw_w_height / 2, anchor=N)

        print('Test geschafft')
        self.loop_key_input()

    def on_press(self, key):
        # print("pressed: {0}".format(key))
        self.drive(key)


    def on_release(self, key):
        # print("released {0}".format(key))
        self.stop_drive(key)

    def loop_key_input(self):
        listener = keyboard.Listener(on_press=self.on_press, on_release=self.on_release)
        listener.start()

    def drive(self, direction):
        # print(self.a_pressed)
        if str(direction) == "'a'" and not self.a_pressed and not self.d_pressed:
            self.a_pressed = True
            self.a_arrow.configure(fg="green")
            print('a')
            cont.turn_left()

        if str(direction) == "'s'" and not self.s_pressed and not self.w_pressed:
            self.s_pressed = True
            self.s_arrow.configure(fg="green")
            print('s')
            cont.drive_back()

        if str(direction) == "'d'" and not self.d_pressed and not self.a_pressed:
            self.d_pressed = True
            self.d_arrow.configure(fg="green")
            print('d')
            cont.turn_right()

        if str(direction) == "'w'" and not self.w_pressed and not self.s_pressed:
            self.w_pressed = True
            self.w_arrow.configure(fg="green")
            print('w')
            cont.drive_forward()

        # print(direction)

    def stop_drive(self, direction):
        if str(direction) == "'a'" and self.a_pressed:
            self.a_pressed = False
            self.a_arrow.configure(fg="black")
            print('stop a')
            cont.stop_turn()

        if str(direction) == "'s'" and self.s_pressed:
            self.s_pressed = False
            self.s_arrow.configure(fg="black")
            print('stop s')
            cont.stop_drive()

        if str(direction) == "'d'" and self.d_pressed:
            self.d_pressed = False
            self.d_arrow.configure(fg="black")
            print('stop d')
            cont.stop_turn()

        if str(direction) == "'w'" and self.w_pressed:
            self.w_pressed = False
            self.w_arrow.configure(fg="black")
            print('stop w')
            cont.stop_drive()

    def destroy_asdw_w(self, _):
        self.asdw_window.destroy()
        self.asdw_window.update()
        State_driving.state = State_driving.State.STOP


class Keybord_Threading(threading.Thread):
    pass


''' was in the loop key input funstion with the listener initialisation

while State_driving.state == State_driving.State.STOP:
    if keyboard.is_pressed("a"):
        print("a")
    if keyboard.is_pressed("s"):
        print("s")
    if keyboard.is_pressed("d"):
        print("d")
    if keyboard.is_pressed("w"):
        print("w")'''