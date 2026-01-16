import tkinter as tk
import os


root = tk.Tk()

# place a label on the root window
message = tk.Label(root, text="Hello, World!")
message.pack()

# change window name

root.title('excellent')

# change window h, w and border

root.geometry('1920x1080+50+50')
root.minsize('600', '400')
root.maxsize('1920', '1080')
# root.iconbitmap(os.path.dirname(os.path.abspath(__file__)) + '/diary_tool_logo.ico')

# path = os.path.dirname(os.path.abspath(__file__)) + '/diary_tool_logo.ico'


# keep the window displaying
root.mainloop()
