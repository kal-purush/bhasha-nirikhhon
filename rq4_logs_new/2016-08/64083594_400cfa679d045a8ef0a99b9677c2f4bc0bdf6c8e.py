#!/usr/bin/python3

from tkinter import *
from tkinter import ttk
from tkinter import messagebox
from PIL import Image, ImageTk

class DMCgui:

    def __init__(self, master):
        
        master.title('Diffusion Monte Carlo Demonstration')
        master.resizable(False, False)
        master.configure(background = '#e8e8e8')
        
        self.style = ttk.Style()
        # self.style.configure('TFrame', background = '#e1d8b9')
        # self.style.configure('TButton', background = '#e1d8b9')
        # self.style.configure('TLabel', background = '#e1d8b9', font = ('Arial', 11))
        self.style.configure('TLabel', font = ('Arial', 11))
        self.style.configure('Header.TLabel', font = ('Arial', 18, 'bold'))      

        self.frame_header = ttk.Frame(master)
        self.frame_header.pack()
        
        ttk.Label(self.frame_header, text = 'Solve harmonic oscillator using DMC', style = 'Header.TLabel').grid(row = 0, column = 0)
        ttk.Label(self.frame_header, wraplength = 300,
                  text = ("Show how DMC works using the harmonic oscillator as an example."
                          "See how walkers evolve with different initial guess of walker distribution.")).grid(row = 1, column = 0)
        
        self.frame_content = ttk.Frame(master)
        self.frame_content.pack()

        self.state = StringVar()
        self.state.set("ground state")
        self.initial_guess = StringVar()
        self.initial_guess.set("uniform")

        ttk.Label(self.frame_content, text = 'Solve for').grid(row = 0, column = 0, padx = 5, sticky = 'sw')
        ttk.Radiobutton(self.frame_content, text = 'ground state', variable = self.state, value = 'ground state').grid(row = 0, column = 1, padx = 5, sticky = 'sw')
        ttk.Radiobutton(self.frame_content, text = '1st excited state', variable = self.state, value = '1st excited state').grid(row = 0, column = 2, padx = 5, sticky = 'sw')
        ttk.Label(self.frame_content, text = 'Initial guess').grid(row = 1, column = 0, padx = 5, sticky = 'sw')
        ttk.Radiobutton(self.frame_content, text = 'uniform', variable = self.initial_guess, value = 'uniform').grid(row = 1, column = 1, padx = 5, sticky = 'sw')
        ttk.Radiobutton(self.frame_content, text = 'gaussian', variable = self.initial_guess, value = 'gaussian').grid(row = 1, column = 2, padx = 5, sticky = 'sw')

        ttk.Button(self.frame_content, text = 'Start',
                   command = lambda:self.start(master)).grid(row = 2, column = 0, padx = 5, pady = 5, sticky = 'e')
        ttk.Button(self.frame_content, text = 'Reset',
                   command = lambda:self.reset(master)).grid(row = 2, column = 1, padx = 5, pady = 5, sticky = 'w')
        
        self.dir="png_g_u"
        self.image = ImageTk.PhotoImage(Image.open(self.dir+'/frame0000.png'))
        self.i = 0
        self.imgLabel = ttk.Label(self.frame_content, image = self.image)
        self.imgLabel.grid(row = 3, column = 0, columnspan = 3)

        self._job = None

    def start(self,master):
        self.dir="png"
        if self.state.get()=="ground state":
          self.dir=self.dir+"_g"
        else:
          self.dir=self.dir+"_e"
        if self.initial_guess.get()=="uniform":
          self.dir=self.dir+"_u"
        else:
          self.dir=self.dir+"_n"
        print(self.dir)
        self.update_image(master)
    
    def reset(self,master):
        if self._job is not None:
            master.after_cancel(self._job)
            self.i = 0
            self._job = None

    def update_image(self,master):
        self.i +=1
        if self.i<100:
          self.image = ImageTk.PhotoImage(Image.open(self.dir+"/frame%.4i.png"%(self.i)))
          self.imgLabel.configure(image = self.image)
          self._job = master.after(50, self.update_image, master)
        else:
          self.reset(master)

         
def main():

    root = Tk()
    dmcgui = DMCgui(root)
    root.mainloop()

if __name__ == "__main__": main()