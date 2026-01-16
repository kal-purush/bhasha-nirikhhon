import tkinter as tk


class TacticVisualiserUI:
    def __init__(self, window: tk.Tk):
        self.window = window
        self.visualiser = tk.Canvas(master=self.window, width=220, height=524, bg='#FFFFFF', bd=0, highlightthickness=0, relief='ridge')
        self.visualiser.grid(row=0, column=0)

        tab1 = buildTacticTab(self.window)

        tab1.place(x=10, y=10)

        tab2 = buildTacticTab(self.window)

        tab2.place(x=10, y=70)


def buildTacticTab(visualiser):
    tab = tk.Canvas(master=visualiser, width=200, height=50, bg='#DCDCDC', bd=0, highlightthickness=0, relief='ridge')
    return tab