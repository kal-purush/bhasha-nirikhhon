import tkinter as tk

class App:
    """Class which is the window of application"""
    def __init__(self, master):
        """Construct new App object
        
        Params:
            master (tkinter): parent of your app"""
        self.__master = master
        self.__master.title("Project")
        self.__master.geometry("700x700")
        self.__master.config(
            background="#A49D8A", highlightthickness="10", highlightcolor="grey"
        )

    def run(self):
        """Method which display the window."""
        self.__master.mainloop()