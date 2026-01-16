import tkinter as tk
from tkinter import Label, Entry
from enum import Enum
from external.constants import DEFAULT_BACKGROUND_COLOR, DEFAULT_FOREGROUND_COLOR, FONT_FAMILY, FONT_SIZE

TEXT_LENGTH = 15

class CustomEntry:
    """Class, which represents custom entry object"""

    background_color = "#FFFFFF"
    """Static property, which represents entry background color"""

    class Mode(Enum):
        """Enumeration type, which represents available modes"""
        NORMAL = 0
        PASSWORD = 1

    def __init__(self, parent_window, x_coordinate, y_coordinate):
        """Construct new CustomEntry object
        
        Params:
            parent_window (Tk): Window, which will be parent of this object
            x_coordinate (int): Coordinate X, which will be used to place object
            y_coordinate (int): Coordinate Y, which will be used to place object"""
        self.__label = Label(
            parent_window,
            width=TEXT_LENGTH,
            font=(FONT_FAMILY, FONT_SIZE),
            background=DEFAULT_BACKGROUND_COLOR,
            foreground=DEFAULT_FOREGROUND_COLOR,
        )
        self.__entry = Entry(
            parent_window,
            width=20,
            borderwidth=0,
            highlightthickness=0,
            background=CustomEntry.background_color,
            foreground=DEFAULT_FOREGROUND_COLOR
        )
        self.move(x_coordinate, y_coordinate)
    
    def move(self, x_coordinate, y_coordinate):
        """Move whole object into new place, specified with coordinates x and y
        Params:
            x_coordinate (int): New x coordinate as a pixel value
            y_coordinate (int): New y coordinate as a pixel value"""
        self.__label.place(x=x_coordinate, y=y_coordinate)
        self.__entry.place(x=(x_coordinate + self.__label.winfo_reqwidth()), y=(y_coordinate-1))

    def set_text(self, new_text):
        """Set new text on the label
        
        Params:
            new_next (str): Text, which will be set"""
        if len(new_text) < TEXT_LENGTH:
            self.__label.config(text=new_text.ljust(TEXT_LENGTH))
        else:
            print(
                "Can't set: '",
                new_text,
                "'. Length must be less than ",
                TEXT_LENGTH,
                " characters",
            )

    def get_text(self) -> str:
        """Provide current text in entry field
        
        Returns:
            Current text from entry field
        """
        return self.__entry.get()

    def set_mode(self, mode):
        """Set a new object's mode
        
        Params:
            mode (CustomEntry.Mode): New mode, which will be set"""
        self.__entry.config(show=("" if mode == CustomEntry.Mode.NORMAL else "*"))