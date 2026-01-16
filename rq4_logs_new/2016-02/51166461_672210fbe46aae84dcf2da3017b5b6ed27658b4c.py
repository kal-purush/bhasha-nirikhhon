'''Location Management Application:
        This program uses algorithms determining search and updates for mobile station movements.

'''

__author__ = 'James Soehlke and David Taylor'
__version__ = '$Revision: 1.0 $'[11:-2]
__date__ = '$Date: 2016/02/06 12:00:00 $'
__copyright__ = 'Copyright (c) 2016 James Soehlke and David N. Taylor'
__license__ = 'Python'

import sys
import Tkinter
from LocationManagementAlgorithms import *

class RSCError(Exception):
    def __init__(self, name, message):
        self.name = name
        self.message = message

    def __str__(self):
        return repr(self.name)

class LocationManagementLabels:
    def __init__(self, parent, col=0, row=-1):
        self.__labels = {}
        self.__col = col
        self.__row = row
        self.__parent = parent
        self.__initialize()

    def __nextRow(self):
        self.__row += 1
        return self.__row

    def __addHeaderLabel(self):
        self.__header = Tkinter.Label(self.__parent, text = "Labels")
        self.__header.grid(column = self.__col, row = self.__nextRow(),  columnspan=2, sticky='EW')

    def __addLabel(self, labelText):
        self.__labels[labelText] = Tkinter.Label(self.__parent,text=labelText, anchor="w",fg="white", bg="blue")
        self.__labels[labelText].grid(column=self.__col, row=self.__nextRow(), columnspan=2, sticky='EW')

    def __initialize(self):
        self.__addHeaderLabel()
        self.__addLabel("Leaf Node 7 CMR")
        self.__addLabel("Leaf Node 8 CMR")
        self.__addLabel("Leaf Node 9 CMR")
        self.__addLabel("Leaf Node 10 CMR")
        self.__addLabel("Leaf Node 11 CMR")
        self.__addLabel("Leaf Node 12 CMR")
        self.__addLabel("Leaf Node 13 CMR")
        self.__addLabel("Leaf Node 14 CMR")
        self.__addLabel("Leaf Node 15 CMR")
        self.__addLabel("Leaf Node 16 CMR")
        self.__addLabel("Leaf Node 17 CMR")
        self.__addLabel("Leaf Node 18 CMR")
        self.__addLabel("minS")
        self.__addLabel("maxS")
        self.__addLabel("MS1: Move to Node")
        self.__addLabel("MS2: Move to Node")

class LocationManagemenEntry:
    def __init__(self, parent, headerName, col, row, setFocus):
        self.__inputs = {}
        self.__entries = {}
        self.__col = col
        self.__row = row
        self.__setFocus = setFocus
        self.__parent = parent
        self.__headerName = "Labels"
        self.__initialize()

    def __nextRow(self):
        self.__row += 1
        return self.__row

    def __addDoubleEntry(self, varName, initValue=0, setFocus=False):
        self.__inputs[varName] = Tkinter.StringVar()
        self.__inputs[varName].set(format(initValue,".2f"))
        self.__entries[varName] = Tkinter.Entry(self.__parent,textvariable=self.__inputs[varName])
        self.__entries[varName].grid(column = self.__col, row = self.__nextRow(), sticky='EW')
        if setFocus:
            self.__entries[varName].focus_set()
            self.__entries[varName].selection_range(0, Tkinter.END)

    def __addIntEntry(self, varName, initValue=0, setFocus=False):
        self.__inputs[varName] = Tkinter.StringVar()
        self.__inputs[varName].set(initValue)
        self.__entries[varName] = Tkinter.Entry(self.__parent,textvariable=self.__inputs[varName])
        self.__entries[varName].grid(column = self.__col, row = self.__nextRow(), sticky='EW')
        if setFocus:
            self.__entries[varName].focus_set()
            self.__entries[varName].selection_range(0, Tkinter.END)

    def __initialize(self):
        self.__header = Tkinter.Label(self.__parent, text = "Values", width = 20)
        self.__header.grid(column = self.__col, row = self.__nextRow(), sticky = "W")
        self.__addDoubleEntry("node7cmr", .3, self.__setFocus)
        self.__addDoubleEntry("node8cmr", .3, self.__setFocus)
        self.__addDoubleEntry("node9cmr", .3, self.__setFocus)
        self.__addDoubleEntry("node10cmr", .3, self.__setFocus)
        self.__addDoubleEntry("node11cmr", .3, self.__setFocus)
        self.__addDoubleEntry("node12cmr", .3, self.__setFocus)
        self.__addDoubleEntry("node13cmr", .3, self.__setFocus)
        self.__addDoubleEntry("node14cmr", .3, self.__setFocus)
        self.__addDoubleEntry("node15cmr", .3, self.__setFocus)
        self.__addDoubleEntry("node16cmr", .3, self.__setFocus)
        self.__addDoubleEntry("node17cmr", .3, self.__setFocus)
        self.__addDoubleEntry("node18cmr", .3, self.__setFocus)
        self.__addDoubleEntry("minS", .6, self.__setFocus)
        self.__addDoubleEntry("maxS", 1.2, self.__setFocus)
        self.__addIntEntry("ms1movetonode", 1)
        self.__addIntEntry("ms2movetonode", 18)

        self.__calcButton = Tkinter.Button(self.__parent,text="Move MS1", command=self.__OnMS1MoveClick)
        self.__calcButton.grid(column=self.__col, row=self.__nextRow())
        self.__calcButton.bind("<Return>", self.__OnMS1MoveClickEvt)

        self.__calcButton = Tkinter.Button(self.__parent,text="Move MS2", command=self.__OnMS2MoveClick)
        self.__calcButton.grid(column=self.__col, row=self.__nextRow())
        self.__calcButton.bind("<Return>", self.__OnMS2MoveClickEvt)

        self.__calcButton = Tkinter.Button(self.__parent,text="MS2 Call MS1", command=self.__OnMS2CallMS1MoveClick)
        self.__calcButton.grid(column=self.__col, row=self.__nextRow())
        self.__calcButton.bind("<Return>", self.__OnMS2CallMS1MoveClickEvt)

        self.__resultVar = Tkinter.StringVar()
        self.__resultVar.set("")
        self.__resultLabel = Tkinter.Label(self.__parent, textvariable = self.__resultVar, width = 20)
        self.__resultLabel.grid(column = self.__col , row=self.__nextRow(), rowspan = 2,)

    def __OnMS1MoveClickEvt(self, evt):
        print "__OnMS1MoveClickEvt"
        self.__OnMS1MoveClick()

    def __OnMS1MoveClick(self):
        print "__OnMS1MoveClick"
        self.__parent.move_ms1(int(self.__inputs["ms1movetonode"].get()))

    def __OnMS2MoveClickEvt(self, evt):
        print "__OnMS2MoveClickEvt"
        self.__OnMS2MoveClick()

    def __OnMS2MoveClick(self):
        print "__OnMS2MoveClick"
        self.__parent.move_ms2(int(self.__inputs["ms2movetonode"].get()))

    def __OnMS2CallMS1MoveClickEvt(self, evt):
        print "__OnMS2CallMS1MoveClickEvt"
        self.__OnMS2CallMS1MoveClick()

    def __OnMS2CallMS1MoveClick(self):
        print "__OnMS2CallMS1MoveClick"
        self.__parent.ms2_call_ms1(self.__inputs)


class LocationManagementApp(Tkinter.Tk):
    def __init__(self,parent):
        Tkinter.Tk.__init__(self,parent)
        self.__parent = parent
        self.__tpa = Tree(PointerAlgorithm())
        self.__tfppa = Tree(ForwardingPointerPAlgorithm())
        self.__tva = Tree(ValueAlgorithm())
        self.__tfpva = Tree(ForwardingPointerVAlgorithm())
        self.__trva = Tree(ReplicationValueAlgorithm())
        self.__trpa = Tree(ReplicationPointerAlgorithm())
        self.__ms1 = MS(1)
        self.__ms2 = MS(2)
        self.__initialize()

    def __initialize(self):
        self.grid()

        fill_tree(self.__tpa)
        fill_tree(self.__tva)
        fill_tree(self.__tfppa)
        fill_tree(self.__tfpva)
        fill_tree(self.__trva)
        fill_tree(self.__trpa)

        self.__tpa.put_ms_into_node_name(self.__ms1, 7)
        self.__tva.put_ms_into_node_name(self.__ms1, 7)
        self.__tfppa.put_ms_into_node_name(self.__ms1, 7)
        self.__tfpva.put_ms_into_node_name(self.__ms1, 7)
        self.__trpa.put_ms_into_node_name(self.__ms1, 7)
        self.__trva.put_ms_into_node_name(self.__ms1, 7)

        self.__tpa.put_ms_into_node_name(self.__ms2, 18)
        self.__tva.put_ms_into_node_name(self.__ms2, 18)
        self.__tfppa.put_ms_into_node_name(self.__ms2, 18)
        self.__tfpva.put_ms_into_node_name(self.__ms2, 18)
        self.__trpa.put_ms_into_node_name(self.__ms2, 18)
        self.__trva.put_ms_into_node_name(self.__ms2, 18)

        self.__numCalcs = Tkinter.IntVar()
        self.__numCalcs.set(1)
        self.__NumCalcsOnPressEnter()

        self.resizable(False, False)
        self.columnconfigure(0, minsize = 250, weight=1)
        self.update()

    def __NumCalcsOnPressEnter(self):
        try:
            if self.__numCalcs.get() > 0:
                self.__calcNames = {}
                for i in range(self.__numCalcs.get()):
                   self.__calcNames[i] = {}
                   self.__calcNames[i][0] = Tkinter.Label(self, text = "Calculator " + str(i+1) + " Name",
                                                          anchor = "w", fg = "white", bg = "blue")
                   self.__calcNames[i][0].grid(column = 0 , row = i, columnspan = 2, sticky = 'EW')
                   self.__calcNames[i][1] = {}
                   self.__calcNames[i][1][0] = Tkinter.StringVar()
                   self.__calcNames[i][1][0].set("Values")
                   self.__calcNames[i][1][1] = Tkinter.Entry(self, textvariable = self.__calcNames[i][1][0], width = 20)
                   self.__calcNames[i][1][1].grid(column = 1, row = i, sticky = 'EW')

                self.__addCalculators()

        except:
            self.__numCalcs.set(1)

    def __addCalculators(self):
        for i in range(self.__numCalcs.get()):
            self.__calcNames[i][0].grid_forget()
            self.__calcNames[i][1][1].grid_forget()

        self.__labels = LocationManagementLabels(self)
        self.__calcs = []
        self.__calcNames[i][1][0].set("")
        self.__calcs.append(LocationManagemenEntry(self, self.__calcNames[0][1][0].get(), i+2, -1, i==0))
        self.update()

    def move_ms1(self, name):
        print "move ms1 to %d" % name

    def move_ms2(self, name):
        print "move ms2 to %d" % name

    def ms2_call_ms1(self, inputs):
        print "ms2 call ms1"
        for k in inputs:
            print inputs[k].get()

if __name__ == "__main__":
    app = LocationManagementApp(None)
    app.title('Location Management')
    app.mainloop()