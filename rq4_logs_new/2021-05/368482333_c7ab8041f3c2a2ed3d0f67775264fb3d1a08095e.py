# Standard/Scientific Calculator

from tkinter import Menu, messagebox, Button, Entry, Tk, Frame
import math

class Calc():

    def __init__(self):
        self.total = 0
        self.current = ''
        self.input_value = True
        self.check_sum = False
        self.op = ''
        self.result = False

    def numberEnter(self, num):
        self.result=False
        firstnum=txtDisplay.get()
        secondnum=str(num)
        if self.input_value:
            self.current = secondnum
            self.input_value = False
        else:
            if secondnum == '.':
                if secondnum in firstnum:
                    return
            self.current = firstnum + secondnum
        self.display(self.current)

    def equals(self):
        try:
            self.result=True
            self.current = float(self.current)
            if self.check_sum == True:
                self.valid_function()
            else:
                self.total = float(txtDisplay.get())
        except:
            self.total = errString

    def display(self,value):
        txtDisplay.delete(0,'end')
        txtDisplay.insert(0,value)

    def valid_function(self):
        try:
            if self.op=='add':
                self.total += self.current

            if self.op=='sub':
                self.total -= self.current

            if self.op=='mult':
                self.total *= self.current

            if self.op=='div':
                self.total /= self.current

            if self.op=='mod':
                self.total %= self.current

            if self.op=='pow':
                self.total **= self.current

            if self.op=='nth':
                self.total = self.current**(1/self.total)
        except:
            self.total = errString

        self.input_value = True
        self.check_sum = False
        self.display(self.total)

    def operation(self,op):
        try:
            if self.check_sum:
                self.valid_function()
            elif not self.result:
                self.total=float(self.current)
                self.input_value=True
            self.check_sum=True
            self.op=op
            self.result=False
        except:
            self.display(errString)

    def clear_entry(self):
        self.result = False
        self.current='0'
        self.display(0)
        self.input_value=True

    def allclear_entry(self):
        self.result=False
        self.clear_entry()
        self.total=0
        self.check_sum = False

    def pi(self):
        self.result=False
        self.current = math.pi
        self.display(self.current)

    def e(self):
        self.result=False
        self.current = math.e
        self.display(self.current)

    def cubrt(self):
        try:
            self.result=False
            if float(txtDisplay.get()) < 0:
                self.current= -((-float(txtDisplay.get()))**(1/3))
            else:
                self.current= (float(txtDisplay.get()))**(1/3)
            self.display(self.current)
        except:
            self.display(errString)

    def fact(self):
        try:
            self.current= math.factorial(int(float(txtDisplay.get())))
            self.display(self.current)
        except:
            self.display(errString)

    def applyFunc(self, func):
        try:
            self.current = func(float(txtDisplay.get()))
            self.display(self.current)
        except:
            self.display(errString)

#main window geometry
root = Tk()
root.title("SciCalc")
root.resizable(width=False, height=False)
root.geometry("460x610")
calc = Frame(root)
calc.grid()
errString = 'Error'

current_value=Calc()
defaultFont = ('Segoe UI Light', 20)

# Top Menu

def modeScientific():
    root.resizable(width=False, height=False)
    root.geometry("740x610")

def modeStandard():
    root.resizable(width=False, height=False)
    root.geometry("460x610")

menubar = Menu(calc)
filemenu = Menu(menubar,tearoff = 0)
filemenu.add_cascade(label = "Standard", command = modeStandard)
filemenu.add_cascade(label = "Scientific", command = modeScientific)
menubar.add_cascade(label = "Change Calculator Mode", menu = filemenu)
menubar.add_cascade(label = "Exit", command = root.destroy)
root.config(menu = menubar)


# Display box

txtDisplay = Entry(calc, font = defaultFont, bd = 1, width = 32, justify = 'right')
txtDisplay.grid(row = 0, column = 0, columnspan = 4, ipady = 30, ipadx = 2)
txtDisplay.insert(0,'0')

# Buttons: Format of each button field: [text (str), width (int), command (function), row (int), column (int)]
b = [['7', 8, lambda : current_value.numberEnter(7), 2, 0],
    ['8', 8, lambda : current_value.numberEnter(8), 2, 1],
    ['9', 8, lambda : current_value.numberEnter(9), 2, 2],
    ['4', 8, lambda : current_value.numberEnter(4), 3, 0],
    ['5', 8, lambda : current_value.numberEnter(5), 3, 1],
    ['6', 8, lambda : current_value.numberEnter(6), 3, 2],
    ['1', 8, lambda : current_value.numberEnter(1), 4, 0],
    ['2', 8, lambda : current_value.numberEnter(2), 4, 1],
    ['3', 8, lambda : current_value.numberEnter(3), 4, 2],
    ['0', 8, lambda: current_value.numberEnter(0), 5, 0],
    ['C', 8, current_value.clear_entry, 1, 0],
    ['AC', 8, current_value.allclear_entry, 1, 1],
    ['mod', 8, lambda: current_value.operation('mod'), 1, 2],
    ['+', 6, lambda: current_value.operation('add'), 1, 3],
    ['-', 6, lambda: current_value.operation('sub'), 2, 3],
    ['*', 6, lambda: current_value.operation('mult'), 3, 3],
    ['÷', 6, lambda: current_value.operation('div'), 4, 3],
    ['.', 8, lambda: current_value.numberEnter('.'), 5, 1],
    ['±', 8, lambda func = (lambda x: -x): current_value.applyFunc(func), 5, 2],
    ['=', 6, current_value.equals, 5, 3],
    ['xʸ', 6, lambda: current_value.operation('pow'), 0, 4],
    ['ˣ√y', 6, lambda: current_value.operation('nth'), 0, 5],
    ['x!', 6, current_value.fact, 0, 6],
    ['inv', 6, lambda func = (lambda x: 1/x): current_value.applyFunc(func), 1, 4],
    ['π', 6, current_value.pi, 1, 5],
    ['e', 6, current_value.e, 1, 6],
    ['cos', 6, lambda func = math.cos: current_value.applyFunc(func), 2, 4],
    ['sin', 6, lambda func = math.sin: current_value.applyFunc(func), 2, 5],
    ['tan', 6, lambda func = math.tan: current_value.applyFunc(func), 2, 6],
    ['acos', 6, lambda func = math.acos: current_value.applyFunc(func), 3, 4],
    ['asin', 6, lambda func = math.asin: current_value.applyFunc(func), 3, 5],
    ['atan', 6, lambda func = math.atan: current_value.applyFunc(func), 3, 6],
    ['rad\n→deg', 6, lambda func = math.degrees: current_value.applyFunc(func), 4, 4],
    ['deg\n→rad', 6, lambda func = math.radians: current_value.applyFunc(func), 4, 5],
    ['sqrt', 6, lambda func = math.sqrt: current_value.applyFunc(func), 4, 6],
    ['log', 6, lambda func = math.log10: current_value.applyFunc(func), 5, 4],
    ['ln', 6, lambda func = math.log: current_value.applyFunc(func), 5, 5],
    ['cubrt', 6, current_value.cubrt, 5, 6]]

for i in b:
    Button(calc, text = i[0], width = i[1], command = i[2], height = 2, font = defaultFont,
           bd = 2, relief='flat').grid(row = i[3], column = i[4])

current_value.allclear_entry()
root.mainloop()