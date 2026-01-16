#-------------------------------------------------------------------------------
# Name:        Ardumower DK
# Purpose:     Communication Ardumower - Windows computer via Bluetooth
#
# Author:      Dragon
#
# Created:     11.08.2015
# Copyright:   (c) Dragon 2015
# Licence:     Just use it. Selling this software might be prohibited.
#
#-------------------------------------------------------------------------------

"""
This recipe describes how to handle asynchronous I/O in an environment where
you are running Tkinter as the graphical user interface. Tkinter is safe
to use as long as all the graphics commands are handled in a single thread.
Since it is more efficient to make I/O channels to block and wait for something
to happen rather than poll at regular intervals, we want I/O to be handled
in separate threads. These can communicate in a threasafe way with the main,
GUI-oriented process through one or several queues. In this solution the GUI
still has to make a poll at a reasonable interval, to check if there is
something in the queue that needs processing. Other solutions are possible,
but they add a lot of complexity to the application.

Created by Jacob Hallén, AB Strakt, Sweden. 2001-10-17
"""


import Tkinter as tk
import time
import threading
import random
import Queue
import statusbar
import Ringbuffer
import serial
import tkMessageBox
import matplotlib
matplotlib.use('TkAgg')
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2TkAgg
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator, FormatStrFormatter

class GuiPart:
    def __init__(self, master, receivedQueue, sendQueue, endCommand, Debug):
        self.master = master
        def donothing():
           filewin = tk.Toplevel(master)
           button = tk.Button(filewin, text="Do nothing button")
           button.pack()

        self.lastCommand = []
        for i in range(100):
            self.lastCommand.append(".")
        ##---------------Menu--------------------------------------------
        menubar = tk.Menu(master)
        filemenu = tk.Menu(menubar, tearoff = 0)
##        filemenu.add_command(label="New", command=donothing)
##        filemenu.add_command(label="Open", command=donothing)
##        filemenu.add_command(label="Save", command=donothing)
##        filemenu.add_command(label="Save as...", command=donothing)
##        filemenu.add_command(label="Close", command=donothing)
##
        filemenu.add_separator()

        filemenu.add_command(label="Exit", command=endCommand)
        menubar.add_cascade(label="File", menu=filemenu)
##        editmenu = tk.Menu(menubar, tearoff=0)
##        editmenu.add_command(label="Undo", command=donothing)
##
##        editmenu.add_separator()

##        editmenu.add_command(label="Cut", command=donothing)
##        editmenu.add_command(label="Copy", command=donothing)
##        editmenu.add_command(label="Paste", command=donothing)
##        editmenu.add_command(label="Delete", command=donothing)
##        editmenu.add_command(label="Select All", command=donothing)
##
##        menubar.add_cascade(label="Edit", menu=editmenu)


        viewmenu = tk.Menu(menubar, tearoff=0)
        viewmenu.add_command(label="Debug", command=Debug)
##        viewmenu.add_command(label="", command=donothing)
##        viewmenu.add_command(label="", command=donothing)
##        viewmenu.add_command(label="", command=donothing)
        menubar.add_cascade(label="View", menu=viewmenu)

        settingsmenu = tk.Menu(menubar, tearoff=0)
        settingsmenu.add_command(label="save", command=lambda: self.send("sz"))
        settingsmenu.add_command(label="load fatory settings", command=lambda: self.send("sx"))
        settingsmenu.add_command(label="Edit", command=lambda: self.send("s"))
        menubar.add_cascade(label="Settings", menu=settingsmenu)

##        helpmenu = tk.Menu(menubar, tearoff=0)
##        helpmenu.add_command(label="Help Index", command=donothing)
##        helpmenu.add_command(label="About...", command=donothing)
##        menubar.add_cascade(label="Help", menu=helpmenu)

        master.config(menu=menubar)
        #---------------------Menu End----------------------------------------------------------
        self.RQueue = receivedQueue
        self.SQueue = sendQueue
        # Set up the GUI

        self.console1 = tk.Button(master, text='Main', command= self.mainmenu)
        self.console1.grid(column = 0, row =0)
        self.console1.grid_remove()
##        console2 = tk.Button(master, text='Refresh', command=lambda: self.send(self.lastCommand[-1],"refresh"))
##        console2.grid(column = 2, row =1)
##        console3 = tk.Button(master, text='Back', command=lambda: self.send(self.lastCommand[-2],"back"))
##        console3.grid(column = 3, row =1)
        self.m1 = statusbar.Meter(root, relief='ridge', bd=3, width = 300)
##        self.m1.grid(row=4, column=1)
        self.nav_buttons = []
        self.scale = []
        self.scaleVar = []
        self.main_buttons = []
        self.maincommand_array = []
        self.maincomName_array = []
        self.titles = []
        coma = "xxx"
        for i in range(10):
            self.main_buttons.append(tk.Button(master, text='-', command = lambda: self.send("")))
            if i >= 5:
                self.main_buttons[i].grid(row =1, column = i-3)
            else:
                self.main_buttons[i].grid(row =0, column = i+1)
            self.main_buttons[i].grid_remove()
        for i in range(35):
            self.nav_buttons.append(tk.Button(master, text='-', command = lambda: self.send("")))
            if i >= 12:
                self.nav_buttons[i].grid(row =i - 6, column = 3)
            else:
                self.nav_buttons[i].grid(row =i + 6, column = 1)
            self.nav_buttons[i].grid_remove()
        for i in range(35):

            self.scaleVar.append(tk.DoubleVar())
            self.scale.append(tk.Scale(master, variable = self.scaleVar[i],  orient='horizontal', resolution=1, from_=0, to= +100))
            if i >= 12:
                self.scale[i].grid(column = 4, row=i-6, sticky = "ew")
            else:
                self.scale[i].grid(column = 2, row=i+6, sticky = "ew")
            self.scale[i].grid_remove()

#        -----------------------------------Plot---------------------------
        self.canvasnumber =4
        self.plot = False
        self.channel = 0
        self.f = Figure()
        self.ax = []
        self.lines =[]
        self.plotylabels=[]
        self.plotxlabels = []
        self.plotnames = []
        self.tdata = [0]
        self.data = [0]
        self.lo = []
        self.hi = []
        for i in range(50):
            self.lo.append(0.0)
            self.hi.append(0.0)
            self.plotnames.append("value")
        self.canvas = []
        self.backgrounds = []
        self.c=FigureCanvasTkAgg(self.f, master=master)
        self.c.get_tk_widget().grid(column=10, row=0, rowspan =20, columnspan = 1)
        self.c.get_tk_widget().grid_remove()
        self.toolbar = NavigationToolbar2TkAgg( self.c, master )
        self.toolbar.grid(column=10, row=30, sticky="w")
        self.toolbar.update()
        self.canvas = []
        for i in range(self.canvasnumber):

            self.backgrounds.append(None)
            self.plotxlabels.append("Time")
            self.plotylabels.append(self.plotnames[i])
            self.ax.append(self.f.add_subplot(11+i+(self.canvasnumber*100)))
            self.lines.append(Line2D(self.tdata, self.data, animated=True))
            self.ax[i].add_line(self.lines[i])
            self.ax[i].set_ylim(-10, 255)
            self.ax[i].set_xlim(1, 300)
            self.ax[i].set_xlabel(self.plotxlabels[i])
            self.ax[i].set_ylabel(self.plotylabels[i])
            self.ax[i].yaxis.set_major_formatter(FormatStrFormatter('%d'))
            self.canvas.append(self.ax[i].figure.canvas)
        self.c.mpl_connect('draw_event',self.update_background)
        self.f.subplots_adjust(hspace=0)
        for a in self.f.axes[:-1]:
            a.set_xlabel("")
            a.set_xticks([])


        self.channel += 1
        self.channel = 0

        self.plotnumbers = [0]
        self.datasize = 300
        self.data_array =[]
        self.datanumber = 0
        self.timeSent = False
        self.idle_flag = True
        self.idle1_flag = True

        # Add more GUI stuff here
        master.bind('<Escape>', self.sendoff)
        master.bind('<Left>', self.sendLeft)
        master.bind('<Right>', self.sendRight)
        master.bind('<Up>', self.sendUp)
        master.bind('<Down>', self.sendDown)
        master.bind('<b>', self.sendB)
        master.bind('<F1>', self.sendF1)
        master.bind('<Prior>', self.sendPrior)
        master.bind('<Next>', self.sendNext)
        master.bind('<a>', self.sendAutomode)



    def update_background(self,event):
        for i in range(len(self.backgrounds)):
            self.backgrounds[i] = self.canvas[i].copy_from_bbox(self.ax[i].bbox)

    def update_plots(self, tdata1=[], data=[], channel=0,dnumber=0, *args):

        if len(tdata1) == len(data):
            if self.backgrounds[channel] is None: return True
            self.canvas[channel].restore_region(self.backgrounds[channel])
            self.lines[channel].set_data(tdata1, data)
    ##        self.ax[channel].set_xlim(min(tdata1), max(tdata1))
            if len(data)>=0:
                lo,hi=float(min(data)), float(max(data))
                if lo <= self.lo[dnumber] or hi>= self.hi[dnumber]:
                    self.lo[dnumber], self.hi[dnumber]=float(lo)-1,float(hi)+1
                    self.ax[channel].set_ylim(self.lo[dnumber], self.hi[dnumber])

                    self.master.after_idle(self.ax[channel].figure.canvas.draw)
                    for i in range(len(self.plotnumbers)):
                        self.master.after_idle(self.ax[i].draw_artist,self.lines[i])
                    for i in range(len(self.plotnumbers)):
                        self.master.after_idle(self.canvas[i].blit,self.ax[i].bbox)


    ##        self.ax[channel].cla()
                else:
                    self.ax[channel].draw_artist(self.lines[channel])
                    self.canvas[channel].blit(self.ax[channel].bbox)
##        print time.time() - t0, "seconds"
        self.toolbar.update()
        self.idle1_flag = True
        return True

    def sendLeft(self,event):
        self.send("nl")
    def sendRight(self,event):
        self.send("nr")
    def sendUp(self,event):
        self.send("nf")
    def sendDown(self,event):
        self.send("ns")
    def sendB(self,event):
        self.send("nb")
    def sendF1(self,event):
        self.send("nm")
    def sendPrior(self,event):
        pass
##        self.send("")
    def sendNext(self,event):
        pass
##        self.send("")
    def sendoff(self,event):
        self.send("ro")

    def sendAutomode(self,event):
        self.send("ra")
    def send(self, command, value = None):

        if value == None and command!= "sz" and command!= "sx": self.lastCommand.append(command)
        if value != None:
            if value == "back":
                if len(self.lastCommand) >= 1: del self.lastCommand[-1]
            elif value== "refresh":
                pass
            else:
                command += "`" + str(value)
        self.SQueue.put(command)
##        print "send", command

    def mainmenu(self):
        self.plot = False
        self.RQueue.put("{.Ardumower (Ardumower)|r~Commands|n~Manual|s~Settings|in~Info|c~Test compass|m1~Log sensors|yp~Plot|y4~Error counters|y9~ADC calibration}init")
        self.send("s")
        self.console1.grid_remove()
        for d in range(len(self.nav_buttons)):
            self.nav_buttons[d].configure(relief = tk.RAISED)

    def setplot(self,buttonpressed):
##        if (len(self.plotnumbers)-1)<=self.canvasnumber:
            for i in range(len(self.nav_buttons)):
                if self.nav_buttons[i].cget("relief") == "ridge":
                    if i not in self.plotnumbers: self.plotnumbers.append(i)
            if buttonpressed not in self.plotnumbers:
                self.plotnumbers.append(buttonpressed)
                self.nav_buttons[buttonpressed].configure(relief = tk.RIDGE)
            else:
                self.plotnumbers.remove(buttonpressed)
                self.nav_buttons[buttonpressed].configure(relief = tk.RAISED)
    ##        print self.plotnumbers
            for lo in self.lo:
                lo = 0.0
            for hi in self.hi:
                hi = -1
            self.plotnumbers.sort()

    ##        print self.plotnumbers
            self.idle_flag = False
            for i in range(self.canvasnumber):
                if len(self.plotnumbers)>= (i+1):
                    try:
                        self.ax[i].set_ylabel(self.plotnames[self.plotnumbers[i]])
                        self.ax[i].set_ylim(self.lo[self.plotnumbers[i]], self.hi[self.plotnumbers[i]])
                        self.master.after_idle(self.ax[i].figure.canvas.draw)
                    except: pass
                else:
                    self.ax[i].set_ylabel("")
                    self.ax[i].set_ylim(0,0)
                    self.master.after_idle(self.ax[i].figure.canvas.draw)
            self.idle_flag = True


    def processIncoming(self):
        """
        Handle all the messages currently in the queue (if any).
        """
        while self.RQueue.qsize():
            msg_array = []
            data_array = []
            self.command_array = []
            self.comName_array = []

            self.multiplier_array = []
            self.scaleActiv_array = []
            self.minimum_array = []
            self.maximum_array = []
            self.ist_array = []
            multiplier = 0
            init = False
            try:
                msg = self.RQueue.get(0)
                # Check contents of message and do what it says
                # As a test, we simply print it

                if msg.find("init") >= 0:
                    msg.strip("init")
                    init = True
                    msg = msg.strip("init")
##                    print init, "incomming"
##                print msg
                msg = msg.strip("\n")
                msg = msg.strip("\r")

                if msg.find("|") >= 0:
                    msg = msg.strip("{")
                    msg = msg.strip("}")
                    msg = msg.strip(".")
                    msg = msg.strip("=")

                    msg_array = msg.rsplit("|")

                elif msg.find(",") >= 0:
                    data_array = msg.rsplit(",")
                    for i in range(len(data_array)):
                        data_array[i] = float(data_array[i])
                else:
##                    print"msg error"
                    pass
                if not self.plot:
                    for i in range(len(self.nav_buttons)):
                        self.nav_buttons[i].grid_remove()
                        self.scale[i].grid_remove()

                if len(msg_array) >= 1 or self.plot:
    ##
                    if init:
                        self.titles = []
##                        print msg_array
                        self.maincommand_array=[]
                        self.maincomName_array = []
                        self.c.get_tk_widget().grid_remove()
##                        self.toolbar.grid_remove()
                        for i in range(len(msg_array)-1):
                            coma, comName = msg_array[i+1].split("~")
    ##                        print coma
                            self.maincommand_array.append(coma)
    ##                        print self.command_array
                            self.maincomName_array.append(comName)
                            self.main_buttons[i].grid()
                            self.main_buttons[i].configure(text=self.maincomName_array[i], command= lambda i=i: self.send(self.maincommand_array[i]))
                            self.titles.append(comName)
                        init = False
##                            self.console1.grid()

                    elif self.plot:
##                        print data_array
                        if len(msg_array)>=2:
##                            print msg_array

                            if msg_array[0].find("`")>=0:
                                title,self.datasize = msg_array[0].split("`")
                            if msg_array[1].find("`")>=0:
                                for i in range(len(msg_array)-1):
                                    msg_array[i+1], num = msg_array[i+1].split("`")
                                print num
                            for name in self.plotnames:
                                name = "value"
                            self.idle_flag = False
                            for i in range(self.canvasnumber):
                                self.ax[i].set_xlim(1, int(self.datasize))
                                self.ax[i].set_ylabel("value")
                                self.ax[i].set_ylim(0,0)
                                self.master.after_idle(self.ax[i].figure.canvas.draw)

                            self.data_array = []
                            self.lo = []
                            self.hi = []
                            self.plotnumbers = [0]
                            for i in range(len(self.main_buttons)):
                                    self.main_buttons[i].configure(relief = tk.RAISED)
##                            print msg_array
##                            print self.titles
                            for tiltle in self.titles:
                                ti = tiltle
                                if msg_array[0].find(ti) >= 0:
                                    tiltlenumber = self.titles.index(tiltle)
                                    self.main_buttons[tiltlenumber].configure(relief = tk.RIDGE)
                            if msg_array[1].find("time")>=0:

                                msg_array = msg_array[1:]
                                print msg_array
                                self.timeSent = True
                            else: self.timeSent = False
                            for i in range(len(msg_array)-1):
                                self.data_array.append(Ringbuffer.RingBuffer(int(self.datasize)))

                                self.hi.append(0.0)
                                self.lo.append(0.0)

                            for i in range(len(self.nav_buttons)):self.nav_buttons[i].grid_remove()
                            for i in range(len(msg_array)-1):
                                self.nav_buttons[i].grid()
                                if i <= (self.canvasnumber-1) :
                                    if i not in self.plotnumbers:self.plotnumbers.append(i)
                                    self.nav_buttons[i].configure(text=msg_array[i+1],relief = tk.RIDGE, command=lambda i= i:self.setplot(i))
                                    self.ax[i].set_ylabel(msg_array[i+1])
                                else:
                                    if i in self.plotnumbers: self.plotnumbers.remove(i)
                                    self.nav_buttons[i].configure(text=msg_array[i+1], relief = tk.RAISED,command=lambda i= i:self.setplot(i))
                                self.plotnames[i] = msg_array[i+1]
##                                print self.plotnames

                            self.idle_flag = True


                        elif len(data_array)>=1:
##                            print data_array
                            t0 = time.time()
                            if self.timeSent:
                                data_array = data_array[1:]
                            for i in range(len(data_array)):
                                self.data_array[i].append(data_array[i])
                            self.datanumber +=1
                            try:
                                if self.idle_flag and self.idle1_flag:
                                    self.idle1_flag = False
##                                    print"number of data: ", self.datanumber
                                    tdata1 = []
                                    if len(self.plotnumbers) >=1:
                                        for i in range(len(self.data_array[self.plotnumbers[0]])):
                                            tdata1.append(i)
                                    for i in range(self.canvasnumber):
                                        if len(self.plotnumbers)>= (i+1):
    ##                                        print len(self.plotnumbers), i
                                            self.channel = i

    ##                                        if (len(self.data_array)-1) >= len(self.data_array[self.plotnumbers[i]]):
                                            self.idle1_flag = False
                                            self.master.after_idle(self.update_plots,tdata1,self.data_array[self.plotnumbers[i]], self.channel,self.plotnumbers[i])
        ##                                        if self.update_plots(tdata1,self.data_array[self.plotnumber+i], self.channel,i):pass

                                    self.datanumber = 0
                            except:
                                pass



                    else:
                        for tiltle in self.titles:
                            if tiltle =="Test compass":
                                ti = "Compass"
                            else:ti = tiltle
                            if msg_array[0].find(ti) >= 0:
                                tiltlenumber = self.titles.index(tiltle)

                                if tiltlenumber == 6:

                                    for button in self.main_buttons:
                                        button.grid_remove()
                                    self.titles=[]

                                    self.c.get_tk_widget().grid()
                                    self.toolbar.grid
                                    self.plot = True
                                    self.maincommand_array = []
                                    self.maincomName_array = []
                                    for d in range(len(self.main_buttons)):
                                            self.main_buttons[d].configure(relief = tk.RAISED)
                                    for i in range(len(msg_array)-1):
                                        coma, comName = msg_array[i+1].split("~")
    ##                                    print coma
                                        self.maincommand_array.append(coma)
    ##                                    print comName
                                        self.maincomName_array.append(comName)

                                        self.main_buttons[i].configure(text=self.maincomName_array[i], command= lambda i=i: self.send(self.maincommand_array[i]))
                                        self.main_buttons[i].grid()

                                        self.titles.append(comName)
                                        self.console1.grid()
                                else:
                                    self.plot = False
                                    for i in range(len(self.main_buttons)):
                                        self.main_buttons[i].configure(relief = tk.RAISED)
                                    self.main_buttons[tiltlenumber].configure(relief = tk.RIDGE)
    ##                        print msg_array[0], tiltle
                        if not self.plot:
                            for i in range(len(msg_array)-1):
                                self.scaleActiv_array.append(False)
                                if msg_array[i+1].find("~ ~") >= 0:
                                    coma, comName, a, multiplier = msg_array[i+1].split("~")
                                    comName, ist , maximum, minimum = comName.split("`")
                                    self.scaleActiv_array[i] = True

                                else:coma, comName = msg_array[i+1].split("~")
            ##                    print coma
                                self.command_array.append(coma)
            ##                    print self.command_array
                                self.comName_array.append(comName)
                                self.multiplier_array.append(0.0)
                                self.minimum_array.append(0)
                                self.maximum_array.append(0)
                                self.ist_array.append(0)
                                self.nav_buttons[i].grid()
                                self.nav_buttons[i].configure(text=comName, command= lambda i=i: self.send(self.command_array[i]))
                                if self.scaleActiv_array[i] == True:
                                    self.multiplier_array[i] = float(multiplier)
                                    self.minimum_array[i] = float(minimum)* self.multiplier_array[i]
                                    self.maximum_array[i] = float(maximum) * self.multiplier_array[i]
                                    self.ist_array[i] = float(ist) * self.multiplier_array[i]
                                    self.scale[i].configure(variable = self.scaleVar[i], resolution=self.multiplier_array[i], from_=self.minimum_array[i], to_= self.maximum_array[i])
                                    self.scale[i].grid()
                                    self.scaleVar[i].set(self.ist_array[i])
                                    self.nav_buttons[i].configure(text=comName, command= lambda i=i: self.send(self.command_array[i], self.scaleVar[i].get()/self.multiplier_array[i]))
            ##                        print "scale ", i




            except Queue.Empty:
                pass

class GuiDebug(tk.Toplevel):
    def __init__(self, master, receivedQueue1, sendQueue):
        tk.Toplevel.__init__(self)
        self.master = master
        self.title("Debug")
        self.geometry('+70+10')
        self.debug_entry_var=tk.StringVar()
        self.debug_entry = tk.Entry(self, textvariable = self.debug_entry_var)
        self.debug_entry.grid(column = 3, row =4)
        self.debug_entry_in_var=tk.StringVar()
        self.debug_entry_in = tk.Entry(self, textvariable = self.debug_entry_in_var)
        self.debug_entry_in.grid(column = 3, row =5, sticky="nesw")
        self.debug_text = tk.Text(self,width=100, height = 20)
        self.debug_entry_var.set("")
        self.debug_text_scrollbar = tk.Scrollbar(self)
        self.debug_text.config(yscrollcommand=self.debug_text_scrollbar.set)
        self.debug_text_scrollbar.config(command=self.debug_text.yview)
        self.debug_text.grid(row=6,column=3,sticky="nesw")
        self.debug_text_scrollbar.grid(row=6,column=3,sticky="nesw")
        def send_debug_command():
            sendQueue.put(self.debug_entry_var.get())
        self.sendbutton = tk.Button(self, text='Send', command= send_debug_command)
        self.sendbutton.grid(column = 3, row =4, sticky = "e")
        self.autoscroll_checkbutton_var = tk.IntVar()
        self.autoscroll_checkbutton = tk.Checkbutton(self, text = "Autoscroll", variable = self.autoscroll_checkbutton_var)
        self.autoscroll_checkbutton.grid(column = 3, row = 7)
        self.withdraw()


class ThreadedClient:
    """
    Launch the main part of the GUI and the worker thread. periodicCall and
    endApplication could reside in the GUI part, but putting them here
    means that you have all the thread controls in a single place.
    """
    def __init__(self, master):
        """
        Start the GUI and the asynchronous threads. We are in the main
        (original) thread of the application, which will later be used by
        the GUI. We spawn a new thread for the worker.
        """
        master.protocol("WM_DELETE_WINDOW", self.endApplication)
        self.master = master

        # Create the queue
        self.received_queue = Queue.Queue()
        self.received_queueDebug = Queue.Queue()
        self.send_queue = Queue.Queue()
        # Set up the GUI part
        def openDebug():
            self.gui_Debug.deiconify()
            self.gui_Debug.lift()
            self.gui_Debug.focus_set()

        def hide_Inout():
            self.gui_Debug.withdraw()
            master.deiconify()

        self.gui = GuiPart(master, self.received_queue, self.send_queue, self.endApplication, openDebug)
        self.gui_Debug = GuiDebug(master, self.received_queueDebug, self.send_queue)
        self.gui_Debug.protocol("WM_DELETE_WINDOW", hide_Inout)
        # Set up the thread to do asynchronous I/O
        # More can be made if necessary
        self.running = False
    	self.threadI = threading.Thread(target=self.initThread)
        self.master.after(1000,self.threadI.start)
        self.threadR = threading.Thread(target=self.receiveThread)
##        self.thread2.start()
        self.threadS = threading.Thread(target=self.sendThread)
##        self.thread3.start()
        self.threadIN = threading.Thread(target=self.gui.processIncoming)
        # Start the periodic call in the GUI to check if the queue contains
        # anything
        self.master.after(1000,self.periodicCall)

    def periodicCall(self):
        """
        Check every 100 ms if there is something new in the queue.
        """
        self.gui.processIncoming()
        self.processIncoming()
        if not self.running:
            # This is the brutal stop of the system. You may want to do
            # some cleanup before actually shutting it down.
            pass

        self.master.after(100, self.periodicCall)

    def processIncoming(self):
        while self.received_queueDebug.qsize():
            try:
                msg = self.received_queueDebug.get()
                self.gui_Debug.debug_entry_in_var.set(msg)
                self.gui_Debug.debug_text.insert(tk.END, msg + "\n")
                if self.gui_Debug.autoscroll_checkbutton_var.get() == 1:
                    self.gui_Debug.debug_text.see(tk.END)
            except Queue.Empty:
                    pass

    def initThread(self):
        """
        This is where we handle the asynchronous I/O. For example, it may be
        a 'select()'.
        One important thing to remember is that the thread has to yield
        control.
        """

        def scan_comport(com_exclude = None):
            com_device = None
            comport = "1"
            while com_device == None and int(comport) <= 50:
                if comport not in com_exclude:
                    try:
                        com_device = serial.Serial("com" + comport, baudrate=115200, writeTimeout = 100)
                        print "Testing com:",comport
                    except:
                        if int(comport) <= 49:
                            comport = str(int(comport) + 1)
##                            print comport
                        else:
                            com_device = None
                            print "Failed to connect to Device"
                            connected = False
##                            print comport
                            comport ="51"
                else:
                    comport = str(int(comport) + 1)
                    print "Testing com:",comport
            return com_device, comport

        print "init"
        com_device = ""
        com = ""
        com_exclude = []

        while com_device != None and self.running == False:
            com_device, com = scan_comport(com_exclude)
##            time.sleep(0.1)
            if com_device != None:
                com_device.write("{.}")
                time.sleep(0.5)
                while com_device.inWaiting() != 0 and self.running == False:
                    muC = com_device.readline()
##                    print muC
                    if muC.find("Ardumower") >= 0:
                        print"found Ardumower"
                        self.Ardumower = com_device
                        msg = muC + "init"
                        self.received_queue.put(msg)
                        msg = ""
                        self.running = True

                        self.threadR.start()
                        self.threadS.start()
            com_exclude.append(com)
        if not self.running:
            self.threadR.start()
            self.threadS.start()

    def receiveThread(self):
        """
        This is where we handle the asynchronous I/O. For example, it may be
        a 'select()'.
        One important thing to remember is that the thread has to yield
        control.
        """
        msg = ""
        if self.running: Mower = self.Ardumower
        while self.running:
            # To simulate asynchronous I/O, we create a random number at
            # random intervals. Replace the following 2 lines with the real
            # thing.
            if Mower.inWaiting() != 0:

                msg += Mower.readline()


                if (msg.find("|") == -1) and (msg.find(",") == -1) and (msg.find("{") == -1) and (msg.find("}") == -1):
                    self.received_queueDebug.put(msg)
                    msg = ""
                elif not msg.find("{")>= 0:
                    self.received_queue.put(msg)
##                    print msg
                    msg = ""

                elif msg.find("}")>= 0:
                    self.received_queue.put(msg)
##                    print msg
                    msg = ""


        if self.running == False:
            msg = "{.Ardumower (Ardumower)|r~Commands|n~Manual|s~Settings|in~Info|c~Test compass|m1~Log sensors|yp~Plot|y4~Error counters|y9~ADC calibration}init"
            self.received_queue.put(msg)
##            msg = "{.Commands`5000|ro~OFF|ra~Auto mode|rc~RC mode|rm~Mowing is OFF|rp~Pattern is RAND|rh~Home|rk~Track|rs~State is OFF |rr~Auto rotate is 0.00|r1~User switch 1 is OFF|r2~User switch 2 is OFF|r3~User switch 3 is OFF}"
####            msg = "|nl~Left|nr~Right|nf~Forward|nb~Reverse|nm~Mow is OFF}"
####            msg = "{.Mow`1000|o00~Overload Counter 0|o01~Power in Watt 0.00|o11~current in mA 0.00|o02~Power max `1000`1000`0~ ~0.1|o03~calibrate mow motor  `0`3000`0~ ~1|o04~Speed 0.00|o05~Speed max `255`255`0~ ~1|o06~Modulate NO|o07~RPM 0|o08~RPM set `3300`4500`0~ ~1|o09p~RPM_P `0`100`0~ ~0.01|o09i~RPM_I `1`100`0~ ~0.01|o09d~RPM_D `1`100`0~ ~0.01|o10~Testing is OFF|o04~for config file: motorMowSenseScale:15.30}"
            time.sleep(5)
            msg = "{.Plot|y7~Sensors|y5~Sensor counters|y3~IMU|y6~Perimeter|y8~GPS|y1~Battery|y2~Odometry2D|y11~Motor control|y10~GPS2D}"
            self.received_queue.put(msg)
            msg ="{=Sensors`300|time s`0|state`1|motL`2|motR`3|motM`4|sonL`5|sonC`6|sonR`7|peri`8|lawn`9|rain`10|dropL`11|dropR`12}"
            time.sleep(5)
            self.received_queue.put(msg)
            for i in range(1000):
                time.sleep(0.5)
                if i%2 == 0 :
                    msg = str(i)+",12,2,0.02,0.10,100,2507,5,1,3,27,25000,0"
##                    print "data:", msg
                if i%2 != 0:
                    msg = "PerChange 216597"
##                    print "dbug:", msg

                if (msg.find("|") == -1) and (msg.find(",") == -1) and (msg.find("{") == -1) and (msg.find("}") == -1):
                    self.received_queueDebug.put(msg)
                    msg = ""
                elif not msg.find("{")>= 0:
                    self.received_queue.put(msg)
##                    print msg
                    msg = ""
                elif msg.find("}")>= 0:
                    self.received_queue.put(msg)
##                    print msg
                    msg = ""

    def sendThread(self):
        """
        This is where we handle the asynchronous I/O. For example, it may be
        a 'select()'.
        One important thing to remember is that the thread has to yield
        control.
        """
        if self.running: Mower = self.Ardumower
        while self.running:
            if self.send_queue.qsize():
                try:
                        # Check contents of message and do what it says
                        #
                    msg = self.send_queue.get(0)
                    Mower.write("{" + msg + "}" + "\n")
                except Queue.Empty:
                    pass
##        while self.running == False:
##            if self.send_queue.qsize():
##                try:
##                        # Check contents of message and do what it says
##                        #
##                    msg = self.send_queue.get(0)
##                    print msg
##                except Queue.Empty:
##                    pass


    def endApplication(self):
        if self.running:
            if tkMessageBox.askokcancel("Quit", "Do you really wish to quit?"):
                self.running = False
                self.master.after(100, self.close_com)
                self.master.after(1000, self.master.destroy)
##                self.master.after(1000, sys.exit)
        else:
            self.master.destroy()

    def close_com(self):
        self.Ardumower.close()

rand = random.Random()
root = tk.Tk()
root.iconbitmap(default='Toy.ico')
root.title('ArdumowerDK')
root.geometry('+50+10')
client = ThreadedClient(root)
root.mainloop()