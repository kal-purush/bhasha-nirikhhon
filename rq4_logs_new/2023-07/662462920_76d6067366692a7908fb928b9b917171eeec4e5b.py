import customtkinter as ctk
from datetime import datetime, timedelta
from tkinter import Tk

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.root = self
        self.root.attributes("-fullscreen",True)
        self.width = 1920
        self.height = 1080
        self.root.grid_rowconfigure(0, weight=1)  # configure grid system
        self.root.grid_columnconfigure(0, weight=1)

        # Color Palletes
        self.mainBg = "cyan"

        # Time Range
        self.startTime = '8:00'
        self.endTime = '20:00'
        self.timeList = self.split_time(self.startTime,self.endTime)
        # Key bindings
        self.bind('<Escape>', lambda e: self.exit(e))

        # Subject List
        self.tables = {}

        self.start_menu()
        #self.main()

    def split_time(self, start, end):
        start_time = datetime.strptime(start, '%H:%M')
        end_time = datetime.strptime(end, '%H:%M')
        interval = timedelta(minutes=15)

        time_list = []
        current_time = start_time

        while current_time <= end_time:
            time_list.append(current_time.strftime('%H:%M'))
            current_time += interval

        return time_list

    def start_menu(self):
        self.startFrame = ctk.CTkFrame(master=self.root, fg_color=self.mainBg)
        self.startFrame.grid(row=0, column=0, padx=0, pady=0, sticky="nsew")

        # Configure Grid
        self.startFrame.rowconfigure(0,weight=2)
        self.startFrame.rowconfigure(1,weight=1)
        self.startFrame.rowconfigure(2,weight=1)

        self.startFrame.columnconfigure(0,weight=1)
        self.startFrame.columnconfigure(1,weight=1)
        self.startFrame.columnconfigure(2,weight=1)

        self.startBtn = ctk.CTkButton(self.startFrame, text="Start", command=lambda: self.setup_menu(), width=350,height=150,font=("Roboto",45))
        self.settingBtn = ctk.CTkButton(self.startFrame, text="Setting", command=lambda: self.setup_menu(), width=350,height=150,font=("Roboto",45))
        self.startBtn.grid(row=1,column=1)
        self.settingBtn.grid(row=2,column=1)

    def setup_menu(self):
        self.startFrame.grid_forget()
        self.settingFrame = ctk.CTkFrame(self.root, fg_color="cyan")
        self.settingFrame.grid(row=0, column=0, padx=0, pady=0, sticky="nsew")
        # self.settingFrame.rowconfigure(1,weight=1)

        self.subjectLabel = ctk.CTkLabel(self.settingFrame,font=("Roboto",25),text_color="black",text="Subject Name:").grid(row=0,column=0,padx=30,pady=30)
        self.subjectEntry = ctk.CTkEntry(self.settingFrame,width=600,font=("Roboto",25)).grid(row=0,column=1, padx=5,pady=20,columnspan=3)
        x_row = 1
        y_row = 0
        for time in (self.timeList):
            check_var = ctk.StringVar(value="on")
            if x_row > 10:
                x_row = 1
                y_row += 1
            else:
                pass

            timeBox = ctk.CTkCheckBox(self.settingFrame, text=time,variable=check_var, text_color="black", onvalue="off", offvalue="on").grid(row=x_row,column=y_row,pady=20)
            x_row += 1
        #self.main()
        self.nextBtn = ctk.CTkButton(self.settingFrame,font=("Roboto",25),text_color="black").grid(row=10,column=3)
        print("Pressed")
    
    def main(self):
        self.mainFrame = ctk.CTkFrame(master=self.root, fg_color=self.mainBg)
        self.mainFrame.grid(row=0, column=0, padx=0, pady=0, sticky="nsew")
        
        self.draw_table(self.mainFrame)

    def draw_table(self,frame):
        days = ['Time/Day','Mon', 'Tue', 'Wed', 'Thu','Fri','Sat','Sun']
        for count, day in enumerate(days):
            dayBox = ctk.CTkLabel(master=frame,width=self.width/8,height=150,fg_color="light grey",font=("Roboto",25),text_color="black",text=day)
            dayBox.grid(row=0,column=count,sticky="nsew")

    def exit(self,e):
        self.root.quit()

app = App()
app.mainloop()
