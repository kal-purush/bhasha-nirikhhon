import serial
import threading
import queue
import csv
import sys
import time
import getpass
import os
import matplotlib
import matplotlib.pyplot as plt
import tkinter as tk
import numpy as np
from tkinter import messagebox
from tkinter import simpledialog

# 16:10 09/02/19 opened on windows
# New trial version of the program with more realistic sampling timings and the addition of averaging.
#  Tied with updated version of embedded code doing the same.

CONSTANT_BOX_NUMBER = "1"

class SerialThread(threading.Thread):

    def __init__(self, toBeReadQueue, tobeWriteQueue, savedCOMPortValue):
        threading.Thread.__init__(self)
        self.readQueue = toBeReadQueue
        self.writeQueue = tobeWriteQueue
        self.localComPortValue = savedCOMPortValue

        if self.localComPortValue == None:
            print("No File or Incorrect Data")
        else:
            print("Data Found")
            if sys.platform == "darwin":
                print("Mac")
                # Analyse
            else:
                # Check contents is COM then letter
                print("Windows")

    def run(self):
        # TODO - Sometimes this is the single letter C. Other times it is the first element. Needs to be the same.
        if (type(self.localComPortValue) is str):
            print("LOAD AGAIN")
            if sys.platform == "darwin":
                s = serial.Serial(self.localComPortValue, 9600) # On my macbook its '/dev/tty.usbmodemHLR46000'
            else:
                s = serial.Serial(self.localComPortValue, 9600) # On my macbook 'COM3'
        else: # TODO - These are the right way to do it for every file load apart from the first one.
            if sys.platform == "darwin":
                s = serial.Serial(self.localComPortValue[0], 9600) # On my macbook its '/dev/tty.usbmodemHLR46000'
            else:
                s = serial.Serial(self.localComPortValue[0], 9600) # On my macbook 'COM3'

        while True:
            if s.inWaiting():
                text = s.readlines(s.inWaiting())
                print(text)
                joined_seq = ''.join(str(v) for v in text)
                print(joined_seq[2:11])

                self.readQueue.put(joined_seq[2:11])

                while self.writeQueue.qsize():
                    localVarToWrite = self.writeQueue.get()
                    print(localVarToWrite)
                    s.write(localVarToWrite)


class App(tk.Tk):
    def __init__(self):
        tk.Tk.__init__(self)
        self.geometry("300x100")
        self.protocol("WM_DELETE_WINDOW", self.handleQuit)
        self.title('Vanuatu GUI (Device ' + CONSTANT_BOX_NUMBER + ')')
        appLines = self.loadInSettings()

        self.temperatureValue = tk.StringVar()
        self.temperatureValueLabel = tk.Label(textvariable=self.temperatureValue)
        self.temperatureValueLabel.grid(row=1, column=1)

        self.temperatureTitleLabel = tk.Label(text='Temperature (Device ' + CONSTANT_BOX_NUMBER + '):')
        self.temperatureTitleLabel.grid(row=1, column=0)

        self.lightValue = tk.StringVar()
        self.lightValueLabel = tk.Label(textvariable=self.lightValue)
        self.lightValueLabel.grid(row=2, column=1)

        self.lightTitleLabel = tk.Label(text='Light (Device ' + CONSTANT_BOX_NUMBER + '):')
        self.lightTitleLabel.grid(row=2, column=0)

        self.stopButton = tk.Button(text="Stop", command=self.stopButtonPressed)
        self.stopButton.grid(row=0, column=0)

        self.startButton = tk.Button(text="Start", command=self.startButtonPressed)
        self.startButton.grid(row=0, column=1)

        self.settingsButton = tk.Button(text="Settings", command=self.settingsButtonPressed)
        self.settingsButton.grid(row=3, column=0)

        self.readQueue = queue.Queue()
        self.writeQueue = queue.Queue()

        self.thread = SerialThread(self.readQueue, self.writeQueue, appLines)
        self.thread.setDaemon(True)

        self.thread.start()
        self.process_serial()

    def process_serial(self):
        while self.readQueue.qsize():
            try:
                localDataFromSerial = self.readQueue.get()
                print("         LOCAL DATA From Ser: " + str(localDataFromSerial))

                localBeginningLetter = localDataFromSerial[0:1]
                print("Beginning Letter " + localBeginningLetter)

                localHexNumber = localDataFromSerial[6:9]
                print("Hex Number " + localHexNumber)

                if localHexNumber and localHexNumber.strip():
                    parsedInDecimal = int(localHexNumber, 16)
                    print("Decimal: " + str(parsedInDecimal))

                    calculatedOn = (5000 / 4096) * parsedInDecimal
                    temperatureCalculated = (calculatedOn - 2103) / - 10.9

                if localBeginningLetter == "A": # Temperature Letter
                    self.temperatureValue.set(str(temperatureCalculated.__round__(2)) + " (Degrees)")

                    if 'parsedInDecimal' in locals():
                        if parsedInDecimal != 0:
                            self.saveData(parsedInDecimal, localBeginningLetter)

                elif localBeginningLetter == "B": # Light Letter in unit of ADC values
                    self.lightValue.set(str(parsedInDecimal) + " (ADC)")

                    if 'parsedInDecimal' in locals():
                        if parsedInDecimal != 0:
                            self.saveData(parsedInDecimal, localBeginningLetter)

                elif localBeginningLetter == "C":
                    scaledDecimalE = parsedInDecimal - 500
                    print("Error Act: ", scaledDecimalE)

                    if 'scaledDecimalE' in locals() and scaledDecimalE != 0:
                        self.saveData(scaledDecimalE, localBeginningLetter)

                elif localBeginningLetter == "D":
                    scaledDecimalI = parsedInDecimal - 500
                    print("Integral Act: ", scaledDecimalI)

                    if 'scaledDecimalI' in locals() and scaledDecimalI != 0:
                        self.saveData(scaledDecimalI, localBeginningLetter)

                elif localBeginningLetter == "E":
                    scaledDecimalD = parsedInDecimal - 500
                    print("Derivitive Act: ", scaledDecimalD)

                    if 'scaledDecimalD' in locals() and scaledDecimalD != 0:
                        self.saveData(scaledDecimalD, localBeginningLetter)

                    if scaledDecimalD in locals() and scaledDecimalE in locals() and scaledDecimalI in locals():
                        # Local Calc for control Variable
                        controlVariable = scaledDecimalE * 0.02 + scaledDecimalI * 0.00 + scaledDecimalD * 4.0
                        print("Control Var: ", controlVariable)

                    #if 'controlVariable' in locals() and controlVariable != 0:
                        #self.saveData(controlVariable, localBeginningLetter)
                elif localBeginningLetter == "U":
                    print("UP")
                    self.saveData("0", localBeginningLetter)
                elif localBeginningLetter == "Z":
                    print("Down")
                    self.saveData("0", localBeginningLetter)
                elif localBeginningLetter == "M":
                    print("Remain")
                    self.saveData("0", localBeginningLetter)
                else:
                    print("Nothing to do")

                if 'parsedInDecimal' in locals():
                    if parsedInDecimal != 0:
                        self.loadGraph()

            except queue.Empty:
                pass
        self.after(100, self.process_serial)

    def handleQuit(self):
        # print("Quiting Program")
        self.destroy()
        # self.readQueue
        plt.close()

    def loadInSettings(self):
        print("Settings")

        if sys.platform == "darwin":

            username = getpass.getuser()

            if os.path.isfile("/Users/" + username + '/VanuatuData/app_settings' + CONSTANT_BOX_NUMBER + '.txt'):
                with open("/Users/" + username + '/VanuatuData/app_settings' + CONSTANT_BOX_NUMBER + '.txt') as f:
                    self.lines = f.readlines()
                    self.lines[0] = self.lines[0].rstrip()
            else:
                print("app settings not there")
                temp = simpledialog.askstring("Welcome", "Please Enter Device Path e.g. /dev/tty.usbmodemHLR46000")
                print("Temp: " + temp)

                if not os.path.exists('/Users/' + username + '/VanuatuData'):
                    os.makedirs('/Users/' + username + '/VanuatuData')

                with open('/Users/' + username + '/VanuatuData/app_settings' + CONSTANT_BOX_NUMBER + '.txt',"a") as f:
                    writer = csv.writer(f,delimiter=",")
                    writer.writerow([temp])

                self.lines = temp

        else:
            if os.path.isfile('C:/Temp/app_settings' + CONSTANT_BOX_NUMBER + '.txt'):
                with open('C:/Temp/app_settings' + CONSTANT_BOX_NUMBER + '.txt') as f:
                    self.lines = f.readlines()
                    self.lines[0] = self.lines[0].rstrip()
            else:
                temp = simpledialog.askstring("Welcome", "Please Enter Device Port e.g. COM3")

                with open('C:/Temp/app_settings' + CONSTANT_BOX_NUMBER + '.txt',"a") as p:
                    writer = csv.writer(p,delimiter=",")
                    writer.writerow([temp])
                    
                self.lines = temp

        #print(" Temp is: " + temp)
        print("Self.lines is: " + str(self.lines))
        return self.lines

# Function to load the graphs on which the data is displayed in real time. Data from graphs is obtained from the save
    # files after it has been placed here by the save functions. If no data is in the files yet then it will fail to
    # load the graphs and try again after the next save to the files. Adjustments to the labels on the graph can also
    # be made inside of this function.
    def loadGraph(self):

        if sys.platform == "darwin":
            #print("Mac")

            username = getpass.getuser()

            try:
                x, y = np.loadtxt('/Users/' + username + '/VanuatuData/data_temperature' + CONSTANT_BOX_NUMBER + '.csv', delimiter=',', unpack=True)
                a, b = np.loadtxt('/Users/'+ username + '/VanuatuData/data_light' + CONSTANT_BOX_NUMBER + '.csv', delimiter=',', unpack=True)
            except:
                print("No Data for this graph, Mac")
        else:
            #print("Windows")
            try:
                x, y = np.loadtxt('C:/Temp/data_temperature' + CONSTANT_BOX_NUMBER + '.csv', delimiter=',', unpack=True)
                a, b = np.loadtxt('C:/Temp/data_light' + CONSTANT_BOX_NUMBER + '.csv' , delimiter=',', unpack=True)
            except:
                print("No Data for graph this time")

        plt.subplot(1,2,1)
        plt.title('Temperature (Device ' + CONSTANT_BOX_NUMBER + ')')
        try:
            plt.plot(x, y, 'b')
        except:
            print("No Data yet")
        plt.xlabel('Time')
        plt.ylabel('Temperature')

        plt.subplot(1,2,2)
        plt.title('Light (Device ' + CONSTANT_BOX_NUMBER + ')')
        try:
            plt.plot(a,b, 'r')
        except:
            print("oops")
        plt.xlabel('Time')
        plt.ylabel('Light Intensity ')

        plt.subplots_adjust(wspace=0.35)

        plt.show(block=False)
        plt.draw()

    def saveData(self, toSaveNumber, toSaveLetter):

        if toSaveLetter == "A": # Temperature value to be saved as ADC, Voltage Value and Degrees

            calculatedOn = (5000 / 4096) * toSaveNumber
            temperatureCalculated = (calculatedOn - 2103) / -10.9

            if sys.platform == "darwin":
                username = getpass.getuser()
                print("Mac")
                with open('/Users/' + username + '/VanuatuData/data_temperature' + CONSTANT_BOX_NUMBER + '.csv',"a") as f:
                    writer = csv.writer(f,delimiter=",")
                    # writer.writerow([time.time(), toSaveNumber, calculatedOn, temperatureCalculated])
                    writer.writerow([time.time(), temperatureCalculated.__round__(2)])
            else:
                with open('C:/Temp/data_temperature' + CONSTANT_BOX_NUMBER + '.csv',"a") as f:
                    writer = csv.writer(f,delimiter=",")
                    # writer.writerow([time.time(), toSaveNumber, calculatedOn, temperatureCalculated])
                    writer.writerow([time.time(), temperatureCalculated.__round__(2)])

        if toSaveLetter == "B": # Light Value to be saved as ADC value
            if sys.platform == "darwin":
                username = getpass.getuser()
                print("Mac")
                with open('/Users/' + username + '/VanuatuData/data_light' + CONSTANT_BOX_NUMBER + '.csv',"a") as p:
                    writer = csv.writer(p,delimiter=",")
                    writer.writerow([time.time(), toSaveNumber])
            else:
                with open('C:/Temp/data_light' + CONSTANT_BOX_NUMBER + '.csv',"a") as p:
                    writer = csv.writer(p,delimiter=",")
                    writer.writerow([time.time(), toSaveNumber])

        if toSaveLetter == "C":  # Error Value to save
            if sys.platform == "darwin":
                username = getpass.getuser()
                print("Mac")
                with open('/Users/' + username + '/VanuatuData/data_error' + CONSTANT_BOX_NUMBER + '.csv',"a") as p:
                    writer = csv.writer(p,delimiter=",")
                    writer.writerow([time.time(), toSaveNumber])
            else:
                with open('C:/Temp/data_error' + CONSTANT_BOX_NUMBER + '.csv',"a") as p:
                    writer = csv.writer(p,delimiter=",")
                    writer.writerow([time.time(), toSaveNumber])

        if toSaveLetter == "D":  # Integral Value to save
            if sys.platform == "darwin":
                username = getpass.getuser()
                print("Mac")
                with open('/Users/' + username + '/VanuatuData/data_integral' + CONSTANT_BOX_NUMBER + '.csv',"a") as p:
                    writer = csv.writer(p,delimiter=",")
                    writer.writerow([time.time(), toSaveNumber])
            else:
                with open('C:/Temp/data_integral' + CONSTANT_BOX_NUMBER + '.csv',"a") as p:
                    writer = csv.writer(p,delimiter=",")
                    writer.writerow([time.time(), toSaveNumber])

        if toSaveLetter == "E":  # Integral Value to save
            if sys.platform == "darwin":
                username = getpass.getuser()
                print("Mac")
                with open('/Users/' + username + '/VanuatuData/data_diff' + CONSTANT_BOX_NUMBER + '.csv',"a") as p:
                    writer = csv.writer(p,delimiter=",")
                    writer.writerow([time.time(), toSaveNumber])
            else:
                with open('C:/Temp/data_diff' + CONSTANT_BOX_NUMBER + '.csv',"a") as p:
                    writer = csv.writer(p,delimiter=",")
                    writer.writerow([time.time(), toSaveNumber])

        if toSaveLetter == "U" or toSaveLetter == "Z" or toSaveLetter == "M":  # Integral Value to save
            if sys.platform == "darwin":
                username = getpass.getuser()
                print("Mac")
                with open('/Users/' + username + '/VanuatuData/data_Movement' + CONSTANT_BOX_NUMBER + '.csv',"a") as p:
                    writer = csv.writer(p,delimiter=",")
                    writer.writerow([time.time(), toSaveLetter])
            else:
                with open('C:/Temp/data_Movement' + CONSTANT_BOX_NUMBER + '.csv',"a") as p:
                    writer = csv.writer(p,delimiter=",")
                    writer.writerow([time.time(), toSaveLetter])

# TODO - Replace only save name for all letters

    def stopButtonPressed(self):
        print("Stop")
        self.writeQueue.put("3".encode())

    def startButtonPressed(self):
        print("Start")

    def settingsButtonPressed(self):
        print("Settings")
        temp = PopupWindow(self)

class PopupWindow(object):
    def __init__(self, master):
        self.top = tk.Toplevel()
        self.entry = tk.Entry(self.top)
        self.entry.grid(row=1, column=1)
        self.top.com_confirm = tk.Button(self.top, text="Save", command=self.take_COM_value)
        self.top.com_confirm.grid(row=1, column=2)
        self.comLabel = tk.Label(self.top, text="COM #")
        self.comLabel.grid(row=1, column=0)

        if sys.platform == "darwin":
            username = getpass.getuser()
            print("Mac")
            with open('/Users/' + username + '/VanuatuData/app_settings' + CONSTANT_BOX_NUMBER + '.txt', "a") as f:
                self.lines = f.readlines()
        else:
            with open('C:/Temp/app_settings' + CONSTANT_BOX_NUMBER + '.txt') as f:
                self.lines = f.readlines()

        self.entry.insert(0, self.lines[0])

    def take_COM_value(self):
        print("Take Com Value")
        print(self.entry.get())

        if sys.platform == "darwin":
            print("Mac port")
            username = getpass.getuser()

            if self.entry.get() != "":
                with open('/Users/' + username + '/VanuatuData/app_settings' + CONSTANT_BOX_NUMBER + '.txt', "a") as p:
                    settingsWriter = csv.writer(p, delimiter=",")
                    settingsWriter.writerow([self.entry.get()])

        else:
            print("Windows COM port")

            # If the COM port field is not empty and contains COM as part of text string then save
            if self.entry.get() != "" and "COM" in self.entry.get():

                with open('C:/Temp/app_settings' + CONSTANT_BOX_NUMBER + '.txt', "a") as p:
                    settingsWriter = csv.writer(p, delimiter=",")
                    settingsWriter.writerow([self.entry.get()])
            else:
                print("Input Invalid. Did not Save")

app = App()
app.mainloop()