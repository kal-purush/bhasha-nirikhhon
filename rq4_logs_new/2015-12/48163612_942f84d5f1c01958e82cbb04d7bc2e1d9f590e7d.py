import Tkinter 
from Tkinter import *
from ScrolledText import *
import tkFileDialog
import tkMessageBox

root = Tkinter.Tk(className = 'Welcome to my first editor')
textPad = ScrolledText(root, width = 100 , height = 80)

def open_command():
	file = tkFileDialog.askopenfile(parent = root, mode = 'rb', title = 'Select a file')
	if file != None:
		contents = file.read()
		textPad.insert('1.0',content)
		file.close()
def save_command():
	file = tkFileDialog.asksaveasfile(mode = 'w')
	if file != None :
		data = self.textPad.get('1.0', END+'-1c')
		file.write(data)
		file.close()
def exit_command():
	if tkMessageBox.askokcancel("Quit","Do you really want to quit ?"):
		root.destroy()
def about_command():
	label = tkMessageBox.showinfo("About","Just Another Textpad \n  copyright \n")

def dummy():
    print "New file is opening..!!"
menu = Menu(root)
root.config(menu=menu)
filemenu = Menu(menu)
menu.add_cascade(label="File", menu=filemenu)
filemenu.add_command(label="New", command=dummy)
filemenu.add_command(label="Open...", command=open_command)
filemenu.add_command(label="Save", command=save_command)
filemenu.add_separator()
filemenu.add_command(label="Exit", command=exit_command)
helpmenu = Menu(menu)
menu.add_cascade(label="Help", menu=helpmenu)
helpmenu.add_command(label="About...", command=about_command)

textPad.pack()
root.mainloop()