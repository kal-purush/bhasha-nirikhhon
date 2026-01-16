import pandas as pd
import os
import sys

if sys.version_info<(3,0,0):
    import Tkinter as tk
    fpath="./Quotes.csv"
    try:
        quotes=pd.read_csv(fpath)
    except:
        raise IOError

else:
    import tkinter as tk
    fpath="./Quotes.csv"
    try:
        quotes=pd.read_csv(fpath)
    except:
        raise IOError


class GUI(tk.Tk):
    def __init__(self,parent):
        tk.Tk.__init__(self,parent)
        self.parent = parent
        self.initialize()

    def initialize(self):
        self.grid()
        self.NameEntryVariable = tk.StringVar()
        self.NameEntry = tk.Entry(self,textvariable=self.NameEntryVariable)
        self.NameEntry.grid(column=0,row=0,sticky='EW')
        self.NameEntry.bind("<Return>", self.OnPressEnter)
        self.NameEntryVariable.set("Enter name here.")

        nameEnterButton = tk.Button(self,text="Enter Name", command=self.Intro)
        nameEnterButton.grid(column=1,row=0)

        self.NameLabel = tk.StringVar()
        NameLabel = tk.Label(self, anchor="w",fg="white",bg="lightblue",textvariable=self.NameLabel)
        NameLabel.grid(columnspan=2,sticky='EW',row=1)

        button2 = tk.Button(self,text="Encourage Me!", command=self.Encourage)
        button2.grid(columnspan=2,sticky='',row=3)

        self.Encouragement = tk.StringVar()
        Encouragement = tk.Label(self, anchor="center", justify="center", fg="black", bg="white", textvariable=self.Encouragement,wraplength=500)
        Encouragement.grid(columnspan=2,sticky='EW',row=4)
        self.Auth = tk.StringVar()
        Auth = tk.Label(self, anchor="se", justify="right", fg="black", bg="white", textvariable=self.Auth,wraplength=200)
        Auth.grid(columnspan=2,sticky='EW',row=5)

        self.grid_columnconfigure(0,weight=1)
        self.resizable(True,False)
        self.NameEntry.focus_set()
        self.NameEntry.selection_range(0, tk.END)

    def Intro(self):
        txt = "Hi, " + self.NameEntryVariable.get()+"! Here's some encouragement for you today."
        self.NameLabel.set(txt)
        self.NameEntry.focus_set()
        self.NameEntry.selection_range(0, tk.END)

    def OnPressEnter(self,event):
        txt = "Hi, " + self.NameEntryVariable.get()+"! Here's some encouragement for you today."
        self.NameLabel.set(txt)
        self.NameEntry.focus_set()
        self.NameEntry.selection_range(0, tk.END)

    def Encourage(self):
        Q = quotes.sample(1)
        q = Q["Quote"].values
        a = Q["Author"].values
        self.Encouragement.set("\""+q[0]+"\"")
        self.Auth.set("- " + a[0])

if __name__ == "__main__":
    app = GUI(None)
    app.title('Encourage Me')
    app.mainloop()