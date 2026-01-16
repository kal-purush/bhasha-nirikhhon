from tkinter import Tk, Label, Button
import configparser
import os
import subprocess
from pir_trigger import *
#from deep_od_lib import *


class DemoGUI:
    def __init__(self, master):
        self.images_num = 0;
        self.master = master

        self.run_demo=False
        #master.geometry("500x500")
        master.title("Counter Demo UI")


        self.config = configparser.ConfigParser()
        os.chdir(os.path.abspath(os.path.dirname(__file__)))
        self.config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'demogui.ini'))
        #print(config['actions']['demo'])

        self.demo_button = Button(master, text="Dont click", command=self.demo, width=10, height=5, font=("TkDefaultFont", 15))
        self.demo_button.grid(row=1, column=0)
        #self.demo_button.pack()

        self.read_button = Button(master, text="Capture", command=self.read, width=10, height=5, font=("TkDefaultFont", 15))
        self.read_button.grid(row=2, column=0)

        self.label_header_people = Label(master, text="People", font=("TkDefaultFont", 20))
        self.label_header_people.grid(row=0,column=1)

        self.label_header_pet = Label(master, text="Pets", font=("TkDefaultFont", 20))
        self.label_header_pet.grid(row=0,column=2)

        self.label_header_vehicle = Label(master, text="Vehicles", font=("TkDefaultFont", 20))
        self.label_header_vehicle.grid(row=0,column=3)

        self.count_people = Label(master, text="0", borderwidth=2, relief="groove", width=3, height=3, font=("TkDefaultFont", 64))
        self.count_people.grid(row=1,column=1, rowspan=2)
        self.count_people.config(bg="gray")

        self.count_pets = Label(master, text="0", borderwidth=2, relief="groove", width=3, height=3, font=("TkDefaultFont", 64))
        self.count_pets.grid(row=1,column=2, rowspan=2)
        self.count_pets.config(bg="gray")

        self.count_vehicles = Label(master, text="0", borderwidth=2, relief="groove", width=3, height=3, font=("TkDefaultFont", 64))
        self.count_vehicles.grid(row=1,column=3, rowspan=2)
        self.count_vehicles.config(bg="gray")






    def addperson(self,n=1):
        self.count_people['text']=int(self.count_people['text'])+n

    def addpet(self,n=1):
        self.count_pets['text']=int(self.count_pets['text'])+n

    def addvehicle(self,n=1):
        self.count_vehicles['text']=int(self.count_vehicles['text'])+n



    def read(self):
        print("read/detect")
        self.run_demo = not self.run_demo
        if self.run_demo:
            self.read_button["text"]="Running..."
            self.master.update()
        else:
            self.read_button["text"]="Read/Detect"
            self.master.update()


    def demo(self):
        print("static demo")
        self.demo_button["text"]="Running..."
        self.master.update()
        camera.capture("static-image.jpg")
        detections = detect_image("static-image.jpg",0.7,gui_callback)
        self.demo_button["text"]="Running..."
        self.demo_button["text"]="Running..."
        display_detections("static-image.jpg",detections,0.7)
        self.demo_button["text"]="Demo"
        self.master.update()

root = Tk()
my_gui = DemoGUI(root)

def gui_callback(detection,confidence):
    if detection=="person":
        my_gui.addperson()
    elif detection=="cat" or detection=="dog":
        my_gui.addpet()
    elif detection=="car" or detection=="bicycle":
        my_gui.addvehicle()

def custom_interval_script():
    #test
    if my_gui.run_demo:
        print("running demo")
        #camera.capture("image"+self.images_num+".jpg")
        wait_capture("image"+str(my_gui.images_num)+".jpg")
        my_gui.images_num += 1;
#        detect_image("image.jpg",0.7,gui_callback)
    root.after(50, custom_interval_script)

root.after(2000, custom_interval_script)
root.mainloop()