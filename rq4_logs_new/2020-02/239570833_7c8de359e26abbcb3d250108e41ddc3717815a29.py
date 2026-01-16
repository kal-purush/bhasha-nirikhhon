import pickle
import tkinter as tk
from tkinter import scrolledtext
TITLE_FONT=('Times New Roman', 24)
BUTTON_FONT = ('Times New Roman', 24)

"""-------------------------------------------------------------------------------------------------------------------------------------"""
class MainMenu(tk.Frame):
    def __init__(self):
        tk.Frame.__init__(self)
        self.lbl_title = tk.Label(text="Game Library", font=TITLE_FONT )
        self.lbl_title.grid(row="0", column = "0", sticky = "news")
        #Buttons
        self.btn_add = tk.Button(text="Add" , font=BUTTON_FONT)
        self.btn_add.grid(row="1", column="0")
        self.btn_edit= tk.Button(text="Edit" , font=BUTTON_FONT)
        self.btn_edit.grid(row="2", column="0")
        self.btn_search = tk.Button(text="Search" , font=BUTTON_FONT)
        self.btn_search.grid(row="3", column="0")
        self.btn_remove = tk.Button(text="Remove" , font=BUTTON_FONT)
        self.btn_remove.grid(row="4", column="0") 
        self.btn_search = tk.Button(text="Save" , font=BUTTON_FONT)
        self.btn_search.grid(row="5", column="0")
"""-------------------------------------------------------------------------------------------------------------------------------------"""
class Saved(tk.Frame):
    def __init__(self):
        tk.Frame.__init__(self)
        self.lbl_success = tk.Label(text = "File Saved.", font=TITLE_FONT).grid(row="0", column="0")
        
        self.btn_ok = tk.Button(text="Ok", font=BUTTON_FONT)
        self.btn_ok.grid(row="1", column="0")
"""-------------------------------------------------------------------------------------------------------------------------------------"""
class EditChooser(tk.Frame):
    def __init__(self):
        tk.Frame.__init__(self)
        self.lbl_instructions = tk.Label(text = "Which title to Edit?", font=TITLE_FONT).grid(row="0", column="0", columnspan="2")
        
        self.ent_title = tk.Entry()
        self.ent_title.grid(row="1", column="0", columnspan="2")
        
        self.btn_cancel = tk.Button(text="Cancel", font=BUTTON_FONT)
        self.btn_cancel.grid(row="2", column="0")
        self.btn_ok = tk.Button(text="Edit", font=BUTTON_FONT)
        self.btn_ok.grid(row="2", column="1")        
"""-------------------------------------------------------------------------------------------------------------------------------------"""
class RemoveChooser(tk.Frame):
    def __init__(self):
        tk.Frame.__init__(self)
        self.lbl_instructions = tk.Label(text = "Which titles to Remove?", font=TITLE_FONT).grid(row="0", column="0", columnspan="2")
        
        self.ent_title = tk.Entry()
        self.ent_title.grid(row="1", column="0", columnspan="2")
        
        self.btn_cancel = tk.Button(text="Cancel", font=BUTTON_FONT)
        self.btn_cancel.grid(row="2", column="0")
        self.btn_ok = tk.Button(text="Remove", font=BUTTON_FONT)
        self.btn_ok.grid(row="2", column="1")        
class info_frame_entries(tk.Frame):
    def __init__(self):
        tk.Frame.__init__(self)    
if __name__ == "__main__":
    datafile = open("game_lib.pickle" , "rb")
    library_database = pickle.load(datafile)
    datafile.close()
    maxkey = max(list(library_database.keys()))
    root = tk.Tk()
    root.title("Game Lib")
    root.geometry("500x500")
    """main_menu = MainMenu()
    main_menu.grid(row="0" , column="0" ,sticky="news")
    
    saved = Saved()
    saved.grid(row="0" , column="0" ,sticky="news")

   
    editchooser = EditChooser()
    editchooser.grid(row="0" ,column="0")
    """
    removechooser = RemoveChooser()
    removechooser.grid(row="0" ,column="0")

    root.mainloop()