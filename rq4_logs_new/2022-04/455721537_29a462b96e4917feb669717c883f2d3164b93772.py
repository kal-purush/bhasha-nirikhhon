# -*- coding: utf-8 -*-
## File = "cmjGuiLibGrid2017Jun23.py"
## Derived from File = "cmjGuiLibGrid2016Jan12.py"
## Derived from File = "cmjGuiLibGrid2015Jul30.py"
## Derived from File = "cmjGuiLibGrid2015Jul29.py"
## Derived from File = "cmjGuiLibGrid2015Mar30.py"
## Derived from File = "cmjGuiLibGrid2015Mar6.py"
## Derived from File = "cmjGuiLib2015Jan14.py"
## Derived from "CrvCounterTest.py"
##
##  A python script to produce a Graphicacl User Interface (GUI)
##  for users to enter scintillation counter test results into 
##  the Mu2e Quality Assurance Data base.
##
##        Written by Merrill Jenkins
##        Department of Physics
##        University of South Alabama
##        Mobile, Alabama 36688
##             Initial version: December 2014
##
##  2015Mar3 - modified by cmj - Use the grid container rather than the pack container
##  2015Mar4 - modified by cmj - Use the new DataLoader module sent by Steve
##  2015Mar30 - modified by cmj - Write to new test tables to test str, int and float
##  2015Mar30 - modified by cmj - Correct saveResult::getStrEntry, saveResult::getFltEntry
##  2015Jul29 - modified by cmj - Add class to write a text frame with Y scrollbars
##  2015Jul30 - modifed by cmj - Add class that constructs a button whose action is to
##				pop up an independent window and start a frame in this window.
##  Modified by cmj 2016Jan12 to use different directories for support modules...
##  Modified by cmj 2017Jun23 to add a scrollList class....
##
#!/usr/bin/env python
##
##  To transmit these changes to the dependent
##  python scripts, complete these steps in the
##  Utilities directory:
##     > rm Utilities.zip
##     > zip -r Utilities.zip *.py
##
import sys
import math
from time import *
from Tkinter import *  # import the python based Tcl interface
from tkMessageBox import askquestion    # dialog box to ask question
from tkSimpleDialog import askstring    # dialog box to ask for a string
sys.path.append("../Utilities/DataLoader.zip")
from myTime import *          # a wrapper to return the time and calander
from DataLoader import DataLoader
#
Version = 'v_2019jan30'
allDiag = 9      # set cmjDiag to this value to turn on all diagnostic prints
cmjDiag = 0      # set non-zero  to print diagnostic prints
               # set to 1 to print the output file results
               # set to 2 to print all diagnostics in saveResults
               # set to 4 to print out the entry diagnositcs
               # set to 6 to print out the check box diagnositcs
               # set to 7 to print out the radioButton diagnostics
               # set to 8 to print out the myLabel diagnostics
               # set to 9 to print out the nextCounter diagnostics
               # set allDiag to turn on all diagnostics
cmjDiagNext = 0          # set to 1 to turn on nextCounter diagnostics
#######################################################################################
#######################################################################################
##    Display date on the GUI.
##
class myDate(Frame):
     def __init__(self,parent=None,myRow=0,myCol=0,myWidth=10):
          Frame.__init__(self,parent)
          self.__row = myRow
          self.__col = myCol
          self.__width = myWidth
          self.__computerTime = myTime()
          self.__computerTime.getComputerTime()
          self.displayDate()
     def getComputerTime(self):         # call when you want current time
          self.__computerTime.getComputerTime()
     def getDay(self):             # reports date when getComputerTime
                                   # was called
          self.__calendarDay = self.__computerTime.getCalendarDate()
          if cmjDiag >= allDiag: print 'self.__calendarDay = %s '%self.__calendarDay
          return(self.__calendarDay)
     def getTime(self):            # reports the time when getComputerTime
                                   # was called
          self.__clockTime = self.__computerTime.getClockTime()
          return(self.__clockTime)
     def displayDate(self):
          self.__dateLabel = Label(self,text=self.getDay(),fg='red',width=self.__width,anchor=W,justify=LEFT)
          self.__dateLabel.grid(row=self.__row,column=self.__col,sticky=W)
#######################################################################################
#######################################################################################
#    Draw the Mu2e logo
class mu2eLogo(Frame):   
     def __init__(self,parent,myRow=0,myCol=0,myWidth=10):
          Frame.__init__(self,parent)
          self.__row = myRow
          self.__col = myCol
          self.__width = myWidth
          self.img=PhotoImage(file='../graphics/mu2e_logo_very_small.gif')
          self.canvas = Canvas(self)
          if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0) :
              print '--------------- mu2eLogo:: self.__row = %d  self__col = %d' %(self.__row,self.__col)
          self.canvas.config(width=self.img.width(),height=self.img.height())
          self.canvas.create_image(2,2,image=self.img,anchor=NW)
          self.canvas.grid(row=self.__row,column=self.__col)
#
#######################################################################################
#######################################################################################
#    Add modal box to get the operator's name....
class Operator(Frame):
     def __init__(self,parent=None):
          Frame.__init__(self,parent)
          self.name = askstring('Startup','Operator Enter Name')
          if cmjDiag >= allDiag : print ' in Operator: self.name %s' % self.name
     def getName(self):
          return(self.name)
     def getLabel(self):
          outLabel = 'Operator is: '+str(self.name)
          return(outLabel)
     def getOperatorName(self):
          return(self.name)
#######################################################################################
#######################################################################################
#  Programming Python. Page 430
from tkMessageBox import askokcancel         # get canned std dialog
class Quitter(Frame):
     def __init__(self,parent,myRow,myCol):         # constructor megthod
          Frame.__init__(self,parent)
          self.__row = myRow
          self.__col = myCol
          widget=Button(self,text='Quit',command=self.quit,bg='red',fg='black')
          widget.grid(row=self.__row,column=self.__col,sticky=SE)
     def quit(self):
          Frame.quit(self)
#
#######################################################################################
#######################################################################################
#    Add modal box to get the barcode...
#    Currently, simulate the bar code reader by reading in a text file
class BarCode(Frame):
     def __init__(self,parent=None):
          Frame.__init__(self,parent)
          self.__input = askquestion('Read Bar Code','Enter')
          if (cmjDiag >= allDiag or cmjDiag == 9): 
               print '!!!! BarCode::__init__...  in BarCode: self.__input %s' % self.__input
          self.__barcode = 'Na'
          self.__counter = -999
     def getBarCode(self):
          temp = self.__barcode
          if(cmjDiag >= allDiag or cmjDiag ==9): 
               print '!!!! BarCode::getBarCode... self_.barcode = %s ' % (self.__barcode)
          return(temp)
     def getCounterNumber(self):
          temp = self.__counter
          if(cmjDiag >= allDiag or cmjDiag ==9): 
               print '!!!! BarCode::getCounterNumber... self_.counter = %s ' % (self.__counter)
          return(temp)
     def initializeBarCodeReader(self):
          barCodeReader = "barcode.txt"
          self.__input = open(barCodeReader,'r')
          self.__m = 0
          self.__storeNumbers =[]
          mm = 0
          for self.__eachLine in self.__input:
               if self.__eachLine[0:1] == "#":  # print comment line
                    if(cmjDiag >= allDiag or cmjDiag == 9):
                         print '%s' % self.__eachLine[1:len(self.__eachLine)-1]
                         print 'eachLine[0:1] = %s' % self.__eachLine[0:1]
               else:               # read contents of barcode
                    if(cmjDiag >= allDiag or cmjDiag == 9):
                         print 'BarCode[%d]:  %s ' % (mm,self.__eachLine)
                    self.__storeNumbers.append(self.__eachLine)
               mm += 1
     def readBarCodeReader(self):
          self.__storeNumbers[self.__m]
          self.__barcode = self.__storeNumbers[self.__m][0:len(self.__storeNumbers[self.__m])-1]
          self.__counter = self.__storeNumbers[self.__m][3:len(self.__storeNumbers[self.__m])-1]
          if (cmjDiag >= allDiag or cmjDiag == 9):
               print 'BarCode::readBarCodeReader.... %s' % self.__storeNumbers[self.__m]
               print 'BarCode::readBarCodeReader... barcode = %s ... counter = %s'  %(self.__barcode,self.__counter)
          self.__m += 1
#
#
#######################################################################################
#######################################################################################
##    A class  to save results from the GUI
##       These results may be retrieved from the class
##       or saved in an output txt file
##
class saveResult(object):
#    def __init_(self):            # the constructor in python is not executed...
#          self.counter
#          self.__barcode
#          self.__operator
#          self.__checkBox
#          self.__Entry
#          self.__CheckBox
#          self.__RadioButton
#	  self.__outFileName
    def setOutputFileName(self,tempFileName):
	  self.__outFileName = 'logFiles/'+tempFileName+strftime('%Y_%m_%d_%H_%M')+'.txt'
    def openFile(self):
	  if self.__outFileName == None:
	      self.__outFileName = 'logFiles/CRV_outfile_'+strftime('%Y_%m_%d_%H_%M')+'.txt'
          self.__tempFile=open(self.__outFileName,'w+')
          if(cmjDiag >= allDiag or cmjDiag ==2): print '----- saveResult::openFile: write to %s' % self.__outFileName
          # define these variables here because they were not defined in the constructor "__init__"
          self.__entry = []
          self.__stringEntry = {}       # A dictionary containg string values
          self.__stringEntryList = []   # A list of saved string dictionaries
          self.__intEntry = {}          # A dictionary containing the integer values
          self.__intEntryList = []      # A list of the saved integer dictionaries
          self.__doubleEntry = {}       # A dictionary of floating point values
          self.__doubleEntryList = []   # A list of the save floating point dictionaries
          self.__checkBox = {}          # dictionary for a set of check boxes
          self.__checkBoxList = [] # list containing a dictionary for each set of check boxes.
          self.__radioButton = {}
          self.__radioButtonList = []   # a list of 
          self.__StringEntry = 0  ## self._StringEntry = 1 means not all entries entered
          self.__IntEntry = 0  ## self._StringEntry = 1 means not all entries entered
          self.__DoubleEntry = 0  ## self._StringEntry = 1 means not all entries entered
          self.__CheckBox = 0 ## self.__CheckBox = 1 means check box not entered
          self.__RadioButton = 0   ## self.__RadioButton = 1 means radio buttons not entered
    def saveOperator(self,temp):       # a method to save the operator name
          self.__operator = temp
    def saveBarCode(self,temp):        # a method to save the barcode information
          self.__barcode = temp
    def saveCounterNumber(self,temp):  # method to save the counter number
	  self.__saveCN = 1  ## set this variable to 0 if the counter number is not used
          self.__counterNumber = temp
    def saveEntry(self,temp):          # a method to save the entry as a string....
          self.__entry.append(temp)
          if(cmjDiag >= allDiag or cmjDiag == 2): print '----- saveResult::saveEntry:: len(self.__entry) = %s'%len(self.__entry)
    def saveStringEntry(self,temp1,temp2):
          self.__stringEntry = {temp1 : temp2}    # save a dictionary of strings
          self.__stringEntryList.append(self.__stringEntry)
          if(cmjDiag >= allDiag or cmjDiag == 2): print '----- saveResult::saveStringEntry:: len(self.__stringEntry) = %s'%len(self.__stringEntry)
    def saveIntEntry(self,temp1,temp2):          # save a dictionary of saved integer values
          self.__intEntry = {temp1: temp2}   # key : value (value is an integer)
          if (cmjDiag >= allDiag or cmjDiag == 2):
            print '----- saveResult::saveIntEntry... temp1 = %s ... temp2 = %s' % (temp1,temp2)
          self.__intEntryList.append(self.__intEntry)
          if(cmjDiag >= allDiag or cmjDiag == 2): print '----- saveResult::saveIntEntry:: len(self.__intEntry) = %d'%len(self.__intEntry)
    def saveFloatEntry(self,temp1,temp2):        # save a dictionary of saved floating values
          self.__doubleEntry = {temp1 : temp2}    # key : value (value is a floating point)
          self.__doubleEntryList.append(self.__doubleEntry)
          if(cmjDiag >= allDiag or cmjDiag == 2): print '----- saveResult::saveDoubleEntry:: len(self.__doubleEntry) = %d'%len(self.__doubleEntry)
    def saveCheckBox(self,temp):  
          self.__checkBox = temp ## this is a dictionary
          self.__checkBoxList.append(self.__checkBox)  # Accumulate a list of dictionaries... Each set of checkboxes has its own dictionary
          if (cmjDiag >= allDiag or cmjDiag == 2):
               print '----- saveResult::saveCheckBox... temp = %s ' % temp
               for entry in sorted(self.__checkBox.keys()):
                    print '----- saveResult::saveCheckBox... key %s ... value %s' %(entry,self.__checkBox[entry])
    def saveRadioButton(self,temp1,temp2):
          if (cmjDiag >= allDiag or cmjDiag == 2): 
               print '----- saveResult::saveRadioButton:  Button = %s value = %s' % (temp1,temp2)
          self.__radioButton ={temp1: temp2}
          self.__radioButtonList.append(self.__radioButton)
    def reviewResults(self):      # Called after saveResults is called to send status string to status bar
          self.__reportString = ''
          if (self.__CheckBox == 0):
               self.__reportString += 'Checkbox not entered. '
          if (self.__RadioButton == 0):
               self.__reportString += 'Radiobutton not selected. '
          if(self.__CheckBox == 1 and self.__RadioButton == 1):
               self.__reportString = 'Counter test successfuly submitted!'
          return(self.__reportString)
    def reset(self):
          self.__entry = []
          self.__intEntry = {}
          self.__intEntryList = []
          self.__doubleEntry = {}
          self.__doubleEntryList = []
          self.__stringEntry = {}
          self.__stringEntryList = []
          self.__checkBox = {}
          self.__checkBoxList = []
          self.__radioButton = {}
          self.__radioButtonList = []
          self.__Entry = 0  ## self._Entry = 1 means not all entries entered
          self.__CheckBox = 0 ## self.__CheckBox = 1 means check box not entered
          self.__RadioButton = 0   ## self.__RadioButton = 1 means radio buttons not entered
    def getOperator(self):
          return(self.__operator)
    def getBarCode(self):
          return(self.__barcode)
    def getCounterNumber(self):
          return(self.__counterNumber)
    def getIntEntry(self,m):                ## get the integer entries from the GUI
          tempInt = -9999.99
          if (cmjDiag >= allDiag or cmjDiag == 1 or cmjDiag ==2):
             print '----- saveResult:getIntEntry... m %s' % m
          for n in self.__intEntryList:
            for mkey in n.keys():
               if mkey == m:
                  tempInt = n[mkey]
	  return tempInt
    def getStrEntry(self,m):                  ## get the string entries from the GUI
          tempString = "NULL"
          if (cmjDiag >= allDiag or cmjDiag == 1 or cmjDiag ==2):
             print '----- saveResult:getStrEntry... m %s' % m
          for n in self.__stringEntryList:
            for mkey in n.keys():
               if mkey == m:
                  tempString = n[mkey]
          return(tempString)
    def getFltEntry(self,m):                ## get the floating point entries from the GUI
          tempFlt = -9999.99
          if (cmjDiag >= allDiag or cmjDiag == 1 or cmjDiag ==2):
             print '----- saveResult:getFltEntry... m %s' % m
          for n in self.__doubleEntryList:
            for mkey in n.keys():
               if mkey == m:
                  tempFlt = n[mkey]
          return(tempFlt)
    def getRadioButton(self,radioKey):     ## get radio button results... which one was pressed
          myRadioResult = -9999
          for n in self.__radioButtonList:
              for myKey in n.keys():
                 if(myKey == radioKey):
                   myRadioResult = n[myKey]
          return(myRadioResult)
    def getCheckBox(self,tempCheckBox):   ## get the check box result... which one (or mult) pressed.
          myCheckBoxResult = -9999
          for n in self.__checkBoxList:
              for myKey in n.keys():
                 if(myKey == tempCheckBox):
                   myCheckBoxResult = n[myKey]
          return(myCheckBoxResult)
    def saveResults(self):
          # construct strings for the output record
          # write output file... eventually output to URL with a json file
          localTime = myTime()
          localTime.getComputerTime()        # time the record is written
          self.__date = localTime.getCalendarDate()
          self.__time = localTime.getClockTime()
          self.__banner0 = '+++++++++++++++++++++++ New Counter +++++++++++++++++++++++++ \n'
          self.__banner1 = self.getOperator()+'\n'
	  self.__banner2 = 'Counter Bar Code: '+self.getBarCode()+'\n'
          self.__banner3 = 'Counter Number:   '+self.getCounterNumber()+' '+self.__date+' '+self.__time+'\n'
          self.__tempFile.write(self.__banner0)
          self.__tempFile.write(self.__banner1)
          self.__tempFile.write(self.__banner2)
          self.__tempFile.write(self.__banner3)
          if (cmjDiag >= allDiag or cmjDiag == 1 or cmjDiag ==2): 
               print '----- saveResult::saveResults... len(self.__entry) = %s' % len(self.__entry)
          if(self.__entry):
               self.__tempFile.write('Entry Results \n')
               for line in sorted(self.__entry):
                    self.__tempFile.write(line+'\n')
          if (cmjDiag >= allDiag or cmjDiag == 1 or cmjDiag ==2): 
               print '----- saveResult::saveResults... len(self.__stringEntryList) = %s' % len(self.__stringEntryList)
          if(self.__stringEntryList):
               self.__tempFile.write('String Results \n')
               for self.__tempString in self.__stringEntryList:
                    for key in self.__tempString.keys():
                         line = ('%s %s \n') % (key,self.__tempString[key])
                         self.__tempFile.write(line)
          if (cmjDiag >= allDiag or cmjDiag == 1 or cmjDiag ==2): 
               print '----- saveResult::saveResults... len(self.__intEntryList) = %s' % len(self.__intEntryList)
          if(self.__intEntryList):
               self.__tempFile.write('Integer Results \n')
               for self.__tempInteger in self.__intEntryList:
                    for key in self.__tempInteger.keys():
                         line =('%s  %d \n') %(key,int(self.__tempInteger[key]))
                         self.__tempFile.write(line)
          if(self.__doubleEntryList):
               self.__tempFile.write('Float Entry Results \n')
               for self.__tempFloat in self.__doubleEntryList:
                    for key in self.__tempFloat.keys():
                         line =('%s  %f \n') %(key,float(self.__tempFloat[key]))
                         self.__tempFile.write(line)
          self.__tempFile.write('Check Box Results \n')
          if (cmjDiag >= allDiag or cmjDiag == 1 or cmjDiag ==2): 
               print '----- saveResult::saveResults... len(self.__checkBoxList) = %s' % len(self.__checkBoxList)
          if self.__checkBoxList:
               for self.__tempCheckBoxSet in self.__checkBoxList:
                    for key in sorted(self.__tempCheckBoxSet.keys()):
                         line = key+' '+str(self.__tempCheckBoxSet[key])+'\n'
                         self.__tempFile.write(line)
               self.__CheckBox = 1
          else:                                        # empty checkBox
               self.__tempFile.write('---------Error --------- No Check buttons selected \n')
          self.__tempFile.write('Radio Button Selected \n')
          if self.__radioButtonList:
               for self.__tempRadioButton in self.__radioButtonList:
                    for key in sorted(self.__tempRadioButton.keys()):
                         line = key+' '+str(self.__tempRadioButton[key])+'\n'
                         self.__tempFile.write(line)
               self.__RadioButton = 1
    def closeFile(self):
          self.__tempFile.close()       # close temporary output file
##
##
##
###########################################################################
###########################################################################
###########################################################################
###########################################################################
##
##   A class to construct a button whose action
##	is to pop up an independent window with
##	as separate python GUI in it.
class myIndependentWindow(Frame):
    def __init__(self,parent=NONE,myRow=0,myCol=0):
      Frame.__init__(self,parent)
      self.__buttonNameText = 'default'
      self.__buttonWidth = 10
      self.__buttonColor = 'red'
      self.__inputText = 'default'
      self.__row = myRow
      self.__col = myCol
      self.__ActionRow = 1
      self.__mySaveIt = saveResult()
      self.__mySaveIt.setOutputFileName('readSipms')
      self.__mySaveIt.openFile()
    def setInputText(self,tempText):
      self.__inputText = tempText
    def setButtonName(self,tempText):
      self.__buttonNameText = tempText
    def setButtonWidth(self,tempWidth):
      self.__buttonWidth = tempWidth
    def setButtonColor(self,tempColor):
      self.__buttonColor = tempColor
    def setAction(self,myFrame):
      self.__localFrame = myFrame
    def makeButton(self):
      self.__localButton = Button(self,text=self.__buttonNameText,width=self.__buttonWidth,anchor=W,justify=LEFT,command=(lambda: self.buttonAction()))
      self.__localButton.config(bg=self.__buttonColor)
      self.__localButton.grid(row=0,column=0,sticky=W)
    def buttonAction(self):
      self.__module = __import__(self.__localFrame)  ## import module here!!!!
      self.__localWindow = Toplevel()			## define an independent top-level window
      self.__localWindow.title(self.__localFrame)	## that contains this module
      self.__localJob = self.__module.packWindow(self.__localWindow,0,0,self.__inputText).grid() 
    def clearButton(self):
      self.__localButton2 = Button(self,text=self.__buttonNameText+'hide',width=self.__buttonWidth,anchor=W,justify=LEFT,command=(lambda: self.buttonClear()))
      self.__localButton2.config(bg='red')
      self.__localButton2.grid(row=2,column=0,sticky=W)
##
###########################################################################
###########################################################################
###########################################################################
###########################################################################
##
##	A class to write scolled text from a file.. 
##   		An instruction text can be inserted
##		on the GUI with this class.
class myScrolledText(Frame):
    def __init__(self,parent=NONE,tempText='DEFAULT',tempFile=None):
      Frame.__init__(self,parent)
      self.__height = 10
      self.__width = 100
      self.__textString = tempText
      self.__bgColor = 'white'
      self.__fgColor = 'black'
      self.__myFont = ('helvetica', 9, 'normal')
      self.__text = Text(self, relief = SUNKEN,height=self.__height)
    def makeWidgets(self):
      self.__scrollBar = Scrollbar(self)
      self.__text = Text(self, relief = SUNKEN,height=10)
      self.__scrollBar.config(command=self.__text.yview)  	## link Y scroll bar and text
      self.__text.config(yscrollcommand=self.__scrollBar.set)	## Move one with the other
      self.__scrollBar.grid(row=0,column=1,sticky=NSEW)		## display the scrollbar
      self.__text.grid(row=0,column=0,sticky=E)			## display the text
    def setText(self,tempText='default',tempFile=None,tempLine=1.0):
      self.__textString = 'secondDefault'
      self.__textLine = tempLine  ## to append a the end of the file: END
				  ## to write at the beginning: '1.0'
      # print 'tempfile = %s \n' % tempFile
      if tempFile != None:
	  self.__textString = open(tempFile,'r').read()		## read a file into the text
      else:
	  self.__textString = tempText
#      self.__text.insert(1.0,self.__textString)			## insert string into text file
      self.__text.insert(self.__textLine,self.__textString)			## insert string into text file
      self.__text.config(height=self.__height)
      self.__text.config(width=self.__width)
      self.__text.config(bg=self.__bgColor,fg=self.__fgColor)
      self.__text.config(font=self.__myFont)
    def setTextBoxHeight(self,tempHeight):
      self.__height = tempHeight
    def setTextBoxWidth(self,tempWidth):
      self.__width = tempWidth
    def setTextColor(self,tempFgColor='black',tempBgColor='white'):
      self.__bgColor = tempBgColor
      self.__fgColor = tempFgColor
    def setTextFont(self,font='helvetica',fontSize=9,fontType='normal'):
      self.__myFont = (font,fontSize,fontType)
##
###########################################################################
###########################################################################
###########################################################################
###########################################################################
##
##   Class to write a label to the GUI
##
class myLabel(Frame):
     def __init__(self,parent=None,myRow=0,myCol=0,width=10):
          Frame.__init__(self,parent)
          self.__labelText = "default"
          self.__font = 'courier'
          self.__fontSize = 9
          self.__fontType = 'normal'
          self.__fontFg = 'black'
          self.__fontBg = 'white'
          self.__width = width
          self.__label = Label(self,text=self.__labelText,width=self.__width,anchor=W,justify=LEFT)
          self.__label.config(font=('courier',10,'bold'))
          self.__label.config(bg='white',fg='black')
          self.__row = myRow
          self.__col = myCol
          self.__label.grid(row=self.__row,column=self.__col)
     def makeLabel(self):              # make the label here... call after setting attributes
          self.__label = Label(self,text=self.__labelText,width=self.__width,anchor=W,justify=LEFT)
          labelfont=(self.__font,self.__fontSize,self.__fontType)
          self.__label.config(font=labelfont)
          self.__label.config(fg=self.__fontFg)
          self.__label.config(bg=self.__fontBg)
          self.__label.grid(row=self.__row,column=self.__col)
          if (cmjDiag >= allDiag or cmjDiag == 8):
               print '***** myLabel::makeLabel... self.labelText = %s ' % self.__labelText 
               print '***** myLabel::makeLabel... self.font      = %s '  %  self.__font
               print '***** myLabel::makeLabel... self.fontSize  = %s '%  self.__fontSize
               print '***** myLabel::makeLabel... self.fontType  = %s '%  self.__fontType
               print '***** myLabel::makeLabel... self.fontFg    = %s ' % self.__fontFg
               print '***** myLabel::makeLabel... self.fontBg    = %s ' % self.__fontBg
               print '***** myLabel::makeLabel... self.__row     = %s ' % self.__row
               print '***** myLabel::makeLabel... self.__col     = %s ' % self.__col
     def setBackgroundColor(self,color):           # set background color of the field
          self.__fontBg = color
	  self.__label.config(bg=self.__fontBg)
     def setForgroundColor(self,color):            # set forground color (i.e. color of font), 'black', 'red', 'blue', 'green'
          self.__fontFg = color
	  self.__label.config(fg=self.__fontFg)
     def setBorder(self,borderWidth=2,borderStyle="flat"):
	  self.__label.config(bd=borderWidth,relief=borderStyle)
     def setText(self,inText):                     # set the default label found in the field
          self.__labelText = inText
          self.__label.config(text=self.__labelText)
     def setSide(self,inSide):                     # set the justification of the text in the field
          self.__label.config(side=inSide)
     def setWidth(self,inWidth):                   # set the width of the field (in characters)
          self.__width = inWidth
     def setHeight(self,inHeight):                 # set the height of the field (in characters)
          self.__label.config(height=inHeight)
     def setFont(self,inFont):                     # set the font type: 'arial', 'Arial', 'Helvetica', 'Times' 
          self.__font = inFont
     def setFontSize(self,inSize):                 # set font size (pnts)
          self.__fontSize = inSize
     def setFontType(self,inType):                 # set font type: 'normal', 'bold', 'italic'
          self.__fontType = inType
     def setFontAll(self,inFont,inSize,inType='normal'):  # set all font features with one call
          self.__font = inFont
          self.__fontSize = inSize
          self.__fontType = inType
	  labelfont=(self.__font,self.__fontSize,self.__fontType)
          self.__label.config(font=labelfont)
##
###########################################################################
###########################################################################
###########################################################################
##    A class to add a label, text entry, and "enter" button
##    This class has a label and an string entry box
##         save inheritance for another day...
##	Converted to grid mode... budget three columns:
##		col0: label, col1:entry, col2: button to enter
class myStringEntry(Frame):
     def __init__(self,parent,myRow,myCol,result):
          Frame.__init__(self,parent)
          self.__name = 'default'
          self.__labelWidth = 10
          self.__entryWidth = 10
          self.__buttonWidth= 10
          self.__entyLabel = ''
          self.__buttonText = 'Enter'
          self.__save_it = result
          self.__row = myRow
          self.__col = myCol
     def makeEntry(self):
          self.initEntry()
          self.getEntry()
     def initEntry(self):          # stack in one row
          self.__entryLabel = '' 
          self.__label = Label(self,width=self.__labelWidth,text=self.__name,anchor=W,justify=LEFT)
          self.__label.grid(row=self.__row,column=self.__col,sticky=W)
          self.__ent = Entry(self,width=self.__entryWidth)
          self.saveEnt(self.__ent)
          self.__var = StringVar()        # associate string variable with entry field
          self.__ent.config(textvariable=self.__var)
          self.__var.set('')
          self.__ent.grid(row=self.__row,column=self.__col+1,sticky=W)
          return 
     def saveEnt(self,temp):
          self.saveEnt = temp
     def getEnt(self):
          return(self.saveEnt)
     def fetch(self,event):
          self.__myName = self.__ent.get()
          self.__temp = event.get()
          self.__var = event.get()
          self.__localTime = myTime()
          self.__localTime.getComputerTime()
          self.__date = self.__localTime.getCalendarDate()
          self.__time = self.__localTime.getClockTime()
          outputLine = str(self.__name)+' '+str(self.__date)+' '+str(self.__time)+' '+str(self.__temp)
          if (cmjDiag >= allDiag or cmjDiag == 4):
               print '--- myStringEntry::fetch... self.__fetch::date = %s ' % self.__date
               print '--- myStringEntry::fetch... self.__fetch::time = %s ' % self.__time
               print '--- myStringEntry::fetch... self.__fetch::self.temp  = %s' % self.__temp
               print '--- myStringEntry::fetch... self.__name =  %s self.__date = %s self.__time = %s self.__temp = %s' % (self.__name,self.__date,self.__time,self.__temp)
               print '--- myStringEntry::fetch... outputLine = %s ' % outputLine
          self.__save_it.saveStringEntry(self.__name,self.__var)
          self.__button.config(bg='yellow')
     def resetEntry(self):                   # reset the entry fields for the test comments...
          tempEnt = self.getEnt()
          tempVar = StringVar()
          tempEnt.config(textvariable=tempVar)
          tempVar.set('')
          self.__button.config(bg='#E3E3E3')
     def getEntry(self):
          self.bind('<Return>',(lambda event:self.fetch(self.__ent)))
          self.__button = Button(self,text=self.__buttonText,width=self.__buttonWidth,anchor=W,justify=LEFT,command=(lambda: self.fetch(self.__ent)))
          self.__button.config(bg='#E3E3E3')
          self.__button.grid(row=self.__row,column=self.__col+2,sticky=W)
          if (cmjDiag >= allDiag or cmjDiag == 4): print '--- myStringEntry::getEntry... after Button in getEntry'
     def getVar(self):
          temp = self.__var.get()
          return(temp)
     def setButtonWidth(self,tempWidth):
          self.__buttonWidth = tempWidth
     def setEntryWidth(self,tempWidth):
          self.__entryWidth = tempWidth
     def setLabelWidth(self,tempWidth):
          self.__labelWidth = tempWidth
     def setButtonText(self,tempText):
          self.__buttonText = tempText+' (str)'
     def setEntryLabel(self,tempLabel):
          self.__entryLabel = tempLabel
     def setEntryText(self,tempName):
          self.__name = tempName

###########################################################################
###########################################################################
###########################################################################
#    A class to add a label, integer entry, and "enter" button
#    This class has a label and an integer entry box
#         save inheritance for another day...
#	Converted to grid mode... budget three columns:
#		col0: label, col1:entry, col2: button to enter
class myIntEntry(Frame):
     def __init__(self,parent,myRow,myCol,result):
          Frame.__init__(self,parent)
          self.__name = 'default'
          self.__labelWidth = 10
          self.__entryWidth = 10
          self.__buttonWidth= 10
          self.__entryLabel = ''
          self.__buttonText = 'Enter'
          self.__save_it = result
          self.__row = myRow
          self.__col = myCol
     def makeEntry(self):
          self.initEntry()
          self.getEntry()
     def initEntry(self):          # stack in one row
          self.__entryLabel = '' 
          self.__label = Label(self,width=self.__labelWidth,text=self.__name,anchor=W,justify=LEFT)
          self.__label.grid(row=self.__row,column=self.__col,sticky=W)
          self.__ent = Entry(self,width=self.__entryWidth)
          if (cmjDiag >= allDiag or cmjDiag == 4):
               print '--- myIntEntry::initEntry... self.__entryWidth = %d' %(self.__entryWidth)
               print '--- myIntEntry::initEntry... self.__labelWidth = %d' %(self.__labelWidth)
          self.saveEnt(self.__ent)
          self.__var = IntVar()      # associate integer variable with entry field
          self.__ent.config(textvariable=self.__var)
          self.__var.set('')
          self.__ent.grid(row=self.__row,column=self.__col+1,sticky=W)
          return self.__ent
     def saveEnt(self,temp):
          self.saveEnt = temp
     def getEnt(self):
          return(self.saveEnt)
     def fetch(self,event):
          self.__myName = self.__ent.get()
          try:                                             # guard against non-integer entries
             self.__var = int(event.get())
          except ValueError :
             self.__var = -9999                            # error value
          self.__localTime = myTime()
          self.__localTime.getComputerTime()
          self.__date = self.__localTime.getCalendarDate()
          self.__time = self.__localTime.getClockTime()
          outputLine = '%s %s %s %d' %(self.__name,self.__date,self.__time,int(self.__var))
          if (cmjDiag >= allDiag or cmjDiag == 4):
               print '--- myIntEntry::fetch... self.fetch::date = %s ' % self.__date
               print '--- myIntEntry::fetch... self.fetch::time = %s ' % self.__time
               print '--- myIntEntry::fetch... self.fetch::self.temp  = %d' % int(self.__temp)
#               print '--- myIntEntry::fetch... self.__name =  %s self.__date = %s self.__time = %s self.__temp = %d' % (self.__name,self.__date,self.__time,int(self.__temp))
               print '--- myIntEntry::fetch... self.__name =  %s self.__date = %s self.__time = %s self.__temp = %d' % (self.__name,self.__date,self.__time,int(self.__var))
               print '--- myIntEntry::fetch... outputLine = %s ' % outputLine
          self.__save_it.saveIntEntry(self.__name,int(self.__var))
          self.__button.config(bg='yellow')
     def resetEntry(self):                   # reset the entry fields for the test comments...
          tempEnt = self.getEnt()
          tempVar = StringVar()
          tempEnt.config(textvariable=tempVar)
          tempVar.set('')
          self.__button.config(bg='#E3E3E3')
     def getEntry(self):
          self.bind('<Return>',(lambda event:self.fetch(self.__ent)))
          self.__button = Button(self,text=self.__buttonText,width=self.__buttonWidth,anchor=W,justify=LEFT,command=(lambda: self.fetch(self.__ent)))
          self.__button.config(bg='#E3E3E3')
          self.__button.grid(row=self.__row,column=self.__col+2,sticky=W)
          if (cmjDiag >= allDiag or cmjDiag == 4): print '--- myIntEntry::getEntry... after Button in getEntry'
     def getVar(self):
          temp = self.__var.get()
          return(temp)
     def setButtonWidth(self,tempWidth):
          self.__buttonWidth = tempWidth
     def setEntryWidth(self,tempWidth):
          self.__entryWidth = tempWidth
     def setLabelWidth(self,tempWidth):
          self.__labelWidth = tempWidth
     def setButtonText(self,tempText):
          self.__buttonText = tempText+' (int)'
     def setEntryLabel(self,tempLabel):
          self.__entryLabel = tempLabel
     def setEntryText(self,tempName):
          self.__name = tempName

###########################################################################
###########################################################################
###########################################################################
#    A class to add a label, float entry, and "enter" button
#    This class has a label and an float entry box
#         save inheritance for another day...
#	Converted to grid mode... budget three columns:
#		col0: label, col1:entry, col2: button to enter
class myFloatEntry(Frame):
     def __init__(self,parent,myRow,myCol,result):
          Frame.__init__(self,parent)
          self.__name = 'default'
          self.__labelWidth = 10
          self.__entryWidth = 10
          self.__buttonWidth= 10
          self.__entyLabel = ''
          self.__buttonText = 'Enter'
          self.__save_it = result
          self.__row = myRow
          self.__col = myCol
     def makeEntry(self):
          self.initEntry()
          self.getEntry()
     def initEntry(self):          # stack in one row
          self.__entryLabel = '' 
          self.__label = Label(self,width=self.__labelWidth,text=self.__name,anchor=W,justify=LEFT)
          self.__label.grid(row=self.__row,column=self.__col,sticky=W)
          self.__ent = Entry(self,width=self.__entryWidth)
          self.saveEnt(self.__ent)
          self.__var = DoubleVar()        # associate double precision variable with entry field
          self.__ent.config(textvariable=self.__var)
          self.__var.set('')
          self.__ent.grid(row=self.__row,column=self.__col+1,sticky=W)
          return self.__ent
     def saveEnt(self,temp):
          self.saveEnt = temp
     def getEnt(self):
          return(self.saveEnt)
     def fetch(self,event):
          self.__myName = self.__ent.get()
          try:                                             # guard against non-floating point entries
             self.__var = float(event.get())
          except ValueError :
             self.__var = -9.999e9                         # error value
          self.__localTime = myTime()
          self.__localTime.getComputerTime()
          self.__date = self.__localTime.getCalendarDate()
          self.__time = self.__localTime.getClockTime()
          outputLine = '%s %s %s %d' %(self.__name,self.__date,self.__time,float(self.__var))
          if (cmjDiag >= allDiag or cmjDiag == 4):
               print '--- myFloatEntry::fetch... self.__fetch::date = %s ' % self.__date
               print '--- myFloatEntry::fetch... self.__fetch::time = %s ' % self.__time
               print '--- myFloatEntry::fetch... self.__fetch::self.temp  = %s' % float(self.__temp)
               print '--- myFloatEntry::fetch... self.__name =  %s self.date = %s self.__time = %s self.__temp = %s' % (self.__name,self.__date,self.__time,self.__var)
               print '--- myFloatEntry::fetch... outputLine = %s ' % outputLine
          self.__save_it.saveFloatEntry(self.__name,float(self.__var))
          self.__button.config(bg='yellow')
     def resetEntry(self):                   # reset the entry fields for the test comments...
          tempEnt = self.getEnt()
          tempVar = StringVar()
          tempEnt.config(textvariable=tempVar)
          tempVar.set('')
          self.__button.config(bg='#E3E3E3')
     def getEntry(self):
          self.bind('<Return>',(lambda event:self.fetch(self.ent)))
          self.__button = Button(self,text=self.__buttonText,width=self.__buttonWidth,anchor=W,justify=LEFT,command=(lambda: self.fetch(self.__ent)))
          self.__button.config(bg='#E3E3E3')
          self.__button.grid(row=self.__row,column=self.__col+2,sticky=W)
          if (cmjDiag >= allDiag or cmjDiag == 4): print '--- myFloatEntry::getEntry... after Button in getEntry'
     def getVar(self):
          temp = self.__var.get()
          return(temp)
     def setButtonText(self,tempText):
          self.__buttonText = tempText+' (flt)'
     def setEntryText(self,tempName):
          self.__name = tempName
     def setEntryLabel(self,tempLabel):
          self.__entryLabel = tempLabel
     def setButtonWidth(self,tempWidth):
          self.__buttonWidth = tempWidth
     def setEntryWidth(self,tempWidth):
          self.__entryWidth = tempWidth
     def setLabelWidth(self,tempWidth):
          self.__labelWidth = tempWidth
##
###########################################################################
###########################################################################
##    A class to add a row of  check boxes
class myRowCheck(Frame):
     def __init__(self,parent, localSaveResults,myRow=0,myCol=0):
          Frame.__init__(self,parent)
          self.__checkTestSet = {} 
          self.__save_it = localSaveResults
          self.__checkText = 'Check Boxes'
          self.__row = myRow
          self.__col = myCol
     def initializeCheckList(self,tempEntry):
          self.__selection = ""
          self.__checkTestSet = tempEntry
          self.__tempVar = {}
          if (cmjDiag >= allDiag or cmjDiag == 6):
               for temp in sorted(self.__checkTestSet.keys()):
                    print '--- myRowCheck::initializeCheckList... selfcheckTestSet = %s' % self.__checkTestSet[temp]
     def makeChecks(self):
          self.__label = Label(self,text=self.__checkText,anchor=W,justify=LEFT)
          self.__label.grid(row=self.__row,column=self.__col,sticky=W,columnspan=len(self.__checkTestSet)-1)
          if (cmjDiag >= allDiag or cmjDiag == 6):
             print '--- myRowCheck::initializeCheckList... self.__row = %d self.__col = %d' % (self.__row,self.__col)
          self.__row =+ 2
          for self.__key in sorted(self.__checkTestSet.keys()):
               self.__var  = IntVar()
               self.__var.set('0')             # initialize all boxes unchecked
               self.__checkbutton = Checkbutton(self,text=self.__key,variable=self.__var,command=self.__checkTestSet[self.__key])
               self.__checkbutton.grid(row=self.__row,column=self.__col,sticky=W)
               self.__col += 1
               if (cmjDiag >= allDiag or cmjDiag == 6):
                    print '--- myRowCheck::initializeCheckList... self.__var.get() = %s ' % self.__var.get()
               self.__tempVar[self.__key] = self.__var
          self.__button = Button(self,text='Enter',command=(lambda :self.changeTestState(self.__key)))
          self.__button.config(bg='#E3E3E3')
          self.__button.grid(row=self.__row,column=self.__col,sticky=W)
     def setText(self,tempText):             # Set the check box group name
          self.__checkText=tempText
##        A method to change the state of the check buttons....
##             that is record which check button is selected or "checked"
     def changeTestState(self,tempKey):
          if (cmjDiag >= allDiag or cmjDiag == 6):
               print '--- myRowCheck::changeTestState... self.__tempVar'
               for nn in sorted(self.__tempVar.keys()):
                    print '--- myRowCheck::changeTestState... Value = %s' % (self.__tempVar[nn].get())
               print '--- myRowCheck::changeTestState... self.__checkTestSet before change'
               for nn in sorted(self.__checkTestSet.keys()):
                    print '--- myRowCheck::changeTestState... self.__checkTestSet.. Value = %s'%(self.__checkTestSet[nn])
          ## Load checkbox values into a dictionary whose values are integers
          for mmm in sorted(self.__checkTestSet.keys()):
               self.__checkTestSet[mmm] = self.__tempVar[mmm].get()
               if (cmjDiag >= allDiag or cmjDiag ==6): 
                    print '**************** self.__tempVar[mmm].get() %s ' % self.__tempVar[mmm]
          if (cmjDiag >= allDiag or cmjDiag == 6):
               print '--- myRowCheck::changeTestState... self.__checkTestSet after change'
               for mm in sorted(self.__checkTestSet.keys()):
                    print '--- myRowCheck::changeTestState... self.__checkTestSet:  Value = %s'%(self.__checkTestSet[mm])
          #self.__save_it.saveCheckBox(self.__checkTestSet)
          self.__button.config(bg='yellow')
          self.__selection = tempKey
          return tempKey
     ##  Allow the user to get the new selection.
     def getSelection(self):
       return self.__selection
     def reset(self):                   # a method to reset the checkboxes for next counter
          if (cmjDiag >= allDiag or cmjDiag == 6): print '--- myRowCheck::reset... self.__vars  '  
          for mm in sorted(self.__checkTestSet.keys()):
               self.__checkTestSet[mm] = 0
          for nn in sorted(self.__tempVar.keys()):
               self.__tempVar[nn].set('0')        # reset the integer variable
          self.__button.config(bg='#E3E3E3')
               
###########################################################################
###########################################################################
##    A class to add a column of check boxes
class myColumnCheck(Frame):
     def __init__(self,parent, localSaveResults,myRow=0,myCol=0):
          Frame.__init__(self,parent)
          self.pack()
          self.__checkTestSet = {} 
          self.__save_it = localSaveResults
          self.__initRow = myRow
          self.__initCol = myCol
          self.__checkText = 'Check Boxes'
          self.__row = myRow
          self.__col = myCol
     def initializeCheckList(self,tempEntry):
          self.__checkTestSet = tempEntry
          self.__tempVar = {}
          if (cmjDiag >= allDiag or cmjDiag == 6):
               for temp in sorted(self.__checkTestSet.keys()):
                    print '--- myColumnCheck::initializeCheckList... selfcheckTestSet = %s' % self.__checkTestSet[temp]
     def makeChecks(self):
          self.__label = Label(self,text=self.__checkText)
          self.__label.grid(row=self.__row,column=self.__col,sticky=W,columnspan=len(self.__checkTestSet)-1)
          if (cmjDiag >= allDiag or cmjDiag == 6):
             print '--- myColumnCheck::initializeCheckList... Label: self.__row = %d self.__col = %d' % (self.__row,self.__col)
          self.__row += 1
          for self.__key in sorted(self.__checkTestSet.keys()):
               self.__var  = IntVar()
               self.__var.set('0')             # initialize all boxes unchecked
               self.__checkbutton = Checkbutton(self,text=self.__key,variable=self.__var,command=self.__checkTestSet[self.__key])
               self.__checkbutton.grid(row=self.__row,column=self.__col,sticky=W)
               self.__row += 1
               if (cmjDiag >= allDiag or cmjDiag == 6):
                    print '--- myColumnCheck::makeChecks.. Check box: self.__row = %d self.__col = %d' % (self.__row,self.__col)
               if (cmjDiag >= allDiag or cmjDiag == 6):
                    print '--- myColumnCheck::makeChecks... self.__var.get() = %s ' % self.__var.get()
               self.__tempVar[self.__key] = self.__var
          self.__button = Button(self,text='Enter',command=(lambda :self.changeTestState(self.__key)))
          self.__button.config(bg='#E3E3E3')
          self.__button.grid(row=self.__row,column=self.__col,sticky=W)
     def setText(self,tempText):             # Set the check box group name
          self.__checkText=tempText
##        A method to change the state of the check buttons....
##             that is record which check button is selected or "checked"
     def changeTestState(self,tempKey):
          if (cmjDiag >= allDiag or cmjDiag == 6):
               print '--- myColumnCheck::changeTestState... self.__tempVar'
               for nn in sorted(self.__tempVar.keys()):
                    print '--- myColumnCheck::changeTestState... Value = %s' % (self.__tempVar[nn].get())
               print '--- myColumnCheck::changeTestState... self.__checkTestSet before change'
               for nn in sorted(self.__checkTestSet.keys()):
                    print '--- myColumnCheck::changeTestState... self.__checkTestSet.. Value = %s'%(self.__checkTestSet[nn])
          ## Load checkbox values into a dictionary whose values are integers
          for mmm in sorted(self.__checkTestSet.keys()):
               self.__checkTestSet[mmm] = self.__tempVar[mmm].get()
               if (cmjDiag >= allDiag or cmjDiag ==6): 
                    print '--- myColumnCheck::changeTestState... self.__tempVar[mmm].get() %s ' % self.__tempVar[mmm]
          if (cmjDiag >= allDiag or cmjDiag == 6):
               print '--- myColumnCheck::changeTestState... self.__checkTestSet after change'
               for mm in sorted(self.__checkTestSet.keys()):
                    print '--- myColumnCheck::changeTestState... self.__checkTestSet:  Value = %s'%(self.__checkTestSet[mm])
          self.__save_it.saveCheckBox(self.__checkTestSet)
          self.__button.config(bg='yellow')
     def reset(self):                   # a method to reset the checkboxes for next counter
          if (cmjDiag >= allDiag or cmjDiag == 6): print '--- myColumnCheck::reset... self.__vars  '  
          for mm in sorted(self.__checkTestSet.keys()):
               self.__checkTestSet[mm] = 0
          for nn in sorted(self.__tempVar.keys()):
               self.__tempVar[nn].set('0')        # reset the integer variable associated with the check buttons!
          self.__button.config(bg='#E3E3E3')
##
###########################################################################
###########################################################################
##    A class to add a row of Radio Buttons...
##         A radio button has a true or false state.
class myRowRadio(Frame):
     def __init__(self,parent,localSaveResults,myRow=0,myCol=0):
          Frame.__init__(self,parent)
#          self.pack()
          self.__radioTestSet = {} 
          self.__save_it = localSaveResults
          self.__radioText = 'Radio Buttons'
          self.__row = myRow
          self.__col = myCol
     def initializeRadioList(self,tempEntry):
          self.__radioTestSet=tempEntry
     def makeRadios(self):
          self.__label = Label(self,text=self.__radioText)
          self.__label.grid(row=self.__row,column=self.__col,sticky=W,columnspan=len(self.__radioTestSet)-1)
          self.__row += 1
          self.__var = StringVar()
          self.__var.set('0')                  # initialze each radio button to unchecked
          for (self.__key,self.__values) in sorted(self.__radioTestSet.items()):
               self.__radioButton = Radiobutton(self,text=self.__key,command=self.onPress,variable=self.__var,value=self.__key)
               self.__radioButton.grid(row=self.__row,column=self.__col,sticky=W)
               if (cmjDiag >= allDiag or cmjDiag == 7):
                     print '--- myRowRadio::makeRadios: self.__row = %d self.__col = %d ' %(self.__row,self.__col)
               self.__col += 1
     def setText(self,tempText):             # set the radio button group name
         self.__radioText=tempText
##
     def onPress(self):                      # branch here if button is pressed
          pick = self.__var.get()
          for self.__key in sorted(self.__radioTestSet.keys()):
               self.__radioTestSet[self.__key] = 0
          self.__radioTestSet[pick] = 1
          if (cmjDiag >= allDiag or cmjDiag == 7):
               print '--- myRowRadio::onPress: you choose: ', pick
               print '--- myRowRadio::onPress: print pick as string %s is chosen' % str(pick)
               print '--- myRowRadio::onPress: key = %s value = %s '%  (pick, self.__radioTestSet[pick])
               print '--- myRowRadio::onPress (use pick as key): key = %s value = %s '%  (pick, self.__radioTestSet[pick])
          temp2 = self.__radioTestSet[pick]    ## value 
          temp1 = str(pick)        ## key
          self.__save_it.saveRadioButton(temp1,temp2)
     def reset(self):                   # a method to reset the radio buttons for next counter
          pick = self.__var.get()
          self.__radioTestSet[pick] = 0
          for (self.__key,self.__values) in sorted(self.__radioTestSet.items()):
               self.__var.set('0')                  # reset the button here......
     def radioReport(self):                       # report the state of the radio button variables
          return
     def getResults(self):                        # report the state of all radio buttons
          if (cmjDiag >= allDiag or cmjDiag == 7):
               print '--- myRowRadio:getResults'
               print '--- myRowRadio:getResults: %s ' % self.__radioTestSet[self.key]
          return(self.__var)    
###########################################################################
###########################################################################
#    A class to add a column of Radio Buttons...
#         A radio button has a true or false state.
class myColumnRadio(Frame):
     def __init__(self,parent,localSaveResults,myRow=0,myCol=0):
          Frame.__init__(self,parent)
          self.__radioTestSet = {} 
          self.__save_it = localSaveResults
          self.__radioText = 'Radio Buttons'
          self.__row = myRow
          self.__col = myCol
          self.__initRow = myRow
          self.__initCol = myCol
     def initializeRadioList(self,tempEntry):
          self.__radioTestSet=tempEntry
          self.__newRow = []
     def makeRadios(self):
          self.__label = Label(self,text=self.__radioText)
          self.__label.grid(row=self.__row,column=self.__col,sticky=W)
          self.__row += 1
          self.__var = StringVar()
          self.__var.set('0')                  # initialze each radio button to unchecked
          for (self.__key,self.__values) in sorted(self.__radioTestSet.items()):
               self.__radio = Radiobutton(self,text=self.__key,command=self.onPress,variable=self.__var,value=self.__key)
               self.__radio.grid(row=self.__row,column=self.__col,sticky=W)
               if (cmjDiag >= allDiag or cmjDiag == 7):
                    print '--- myColumnRadio::makeRadios: self.__row = %d self.__col = %d ' %(self.__row,self.__col)
               self.__row += 1
     def setText(self,tempText):             # set the radio button group name
         self.__radioText=tempText
##
     def onPress(self):                      # branch here if button is pressed
          pick = self.__var.get()
          for self.__key in sorted(self.__radioTestSet.keys()):
               self.__radioTestSet[self.__key] = 0
          self.__radioTestSet[pick] = 1
          if (cmjDiag >= allDiag or cmjDiag == 7):
               print '--- myColumnRadio::onPress: you choose: ', pick
               print '--- myColumnRadio::onPress: print pick as string %s is chosen' % str(pick)
               print '--- myColumnRadio::onPress: key = %s value = %s '%  (pick, self.__radioTestSet[pick])
               print '--- myColumnRadio::onPress (use pick as key): key = %s value = %s '%  (pick, self.__radioTestSet[pick])
          temp2 = self.__radioTestSet[pick]    ## value 
          temp1 = str(pick)        ## key
          self.__save_it.saveRadioButton(temp1,temp2)
     def reset(self):                   # a method to reset the radio buttons for next counter
          pick = self.__var.get()
          self.__radioTestSet[pick] = 0
          for (self.__key,self.values) in sorted(self.__radioTestSet.items()):
               self.__var.set('0')                  # reset the button here......
     def radioReport(self):                       # report the state of the radio button variables
          return
     def getResults(self):                        # report the state of all radio buttons
          if (cmjDiag >= allDiag or cmjDiag == 7):
               print '--- myColumnRadio:getResults'
               print '--- myColumnRadio:getResults: %s ' % self.__radioTestSet[self.key]
          return(self.var)    
##
##
##
###########################################################################
###########################################################################
###########################################################################
###########################################################################
##
##   A class to construct a scrollList
##	This class will add to a comma-separted string any entries
##	that are double clicked....
##   There is a fetching accessor function to get the final string
##	after all selections are made.
##   The selections are passed as a list when the class is called.
# #
class ScrolledList(Frame):
	def __init__(self,parent,options,tempWidth=20):
		Frame.__init__(self,parent)
		self.pack(expand=YES,fill=BOTH)
		self.makeWidgets(options,tempWidth)
		self.__recordSelection = ""
	def handleList2(self,event):				# Run on double left clidk	
		index = self.listbox.curselection()		# on list double-click
		label = self.listbox.get(index)			# fetch selection text
		self.listbox.itemconfig(index,bg="yellow")	## cmj2018Oct1.. selected color.
		self.runCommand2(label)				# and call action here
	def makeWidgets(self,options,tempWidth):				# or get (ACTIVE)
		sbar = Scrollbar(self)
		list = Listbox(self,relief=SUNKEN,width=tempWidth)
		sbar.config(command=list.yview)			# cross link sbar and list...
		list.config(yscrollcommand=sbar.set)		# move one moves the other
		sbar.pack(side=RIGHT,fill = Y)			# pack first = clip last
		list.pack(side=LEFT, expand=YES,fill=BOTH)	# list clipped first
		pos = 0
		for label in options:				# Add to list box
			list.insert(pos,label)			# Set event handler
			pos += 1
		list.config(selectmode=SINGLE, setgrid = 1)
		list.bind('<Double-1>',self.handleList2)		# double left click
		#list.bind('<Single-1>',self.handleList1)		# single left click
		self.listbox=list				
	def runCommand2(self,selection):
		#print '(double left click) You selected: ',selection
		#self.__recordSelection.append(selection)
		self.__recordSelection = self.__recordSelection+selection+","  ## build output string...
		#print ("***scrollList:runCommand2: self.__recordSelection = %s \n") %(self.__recordSelection)
		return selection
	def fetchList(self):					# Offer an accessor to get the list selected
		return self.__recordSelection 
##
##
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################     
#######################################################################################
#######################################################################################
##
##	This class is the main class that sets up the GUI
##      window amd checks all supplied widgets.  The User enters the various quantities:
##	string values, integer values, float values, check boxex and radio boxes
##
##	A "next counter" method button is setup... When clicked this button
##	  will cause the entered data to be sent to the data base.
##        Then all fields are cleared and ready for the next counter or
##        Device test results to be entered.
##
##     This class contains the functions needed to build the
##        strings to send the data to the database.  This class uses the 
##        client class (DataLoader) developed by the Fermilab data base group 
##        (Steven White, etc.).  THIS FEATURE IS DISABLED AS THE TEST DATA BASE IS NOT
##        SETUP TO RECIEVE ALL DATA TYPES AT THIS TIME
##
##	Later input from USB devices (such as the barcode reader) will
##        read out in this class to be entered in the database when
##        the 'next counter" button is clicked.
##
##      Invoking this file runs this class... 
##              $ python cmjGuiLibGrid2015Mar3G.py
##      A second class (packSmallWindow) is provided later to test the database
##        writing as currently setup.
##	Convert from the packing geometry to the grid Geometry by cmj 2014Mar3
##.
class packWindow(Frame):
     def __init__(self,parent=NONE,myRow=0,myCol=0):
          Frame.__init__(self,parent)
          if (cmjDiag >= allDiag or cmjDiag == 100): print 'XXXX packWindow::__main__  Enter driving method '
          self.__mySaveIt = saveResult()
          self.__mySaveIt.openFile()
          self.__row = myRow
          self.__col = myCol
##         Get the operator name from a modal window...
##
##           These labels span multiple columns
##             To span multiple columns, columnspan in grid call must be set, then
##             the label width must be set to match the spacing
          self.__operator=Operator(self)  # include in row
          self.__operatorName = self.__operator.getLabel()
          self.__mySaveIt.saveOperator(self.__operatorName)
          self.__operatorLabel = myLabel(self,self.__row,self.__col)
          self.__operatorLabel.setForgroundColor('black')
          self.__operatorLabel.setText(self.__operatorName)
          self.__operatorLabel.setFontAll('Arial',12,'bold')
          self.__operatorLabel.setWidth(55)
          self.__operatorLabel.makeLabel()
          self.__operatorLabel.grid(row=self.__row,column=self.__col,columnspan=3,sticky=W)
          self.__row += 1
          if (cmjDiag >= allDiag or cmjDiag == 100): print 'XXXX packWindow::__init__: self.__operator = %s' % self.__operatorName
##        Get the Bar Code number (therefore the counter number)
##            Later on use the barcode reader... this involves a USB reader
          self.__barCode = BarCode()
          self.__barCode.initializeBarCodeReader()
          self.__barCode.readBarCodeReader()
          self.__barCodeNumber = self.__barCode.getBarCode()
          self.__mySaveIt.saveBarCode(self.__barCodeNumber)
          self.__mySaveIt.saveCounterNumber(self.__barCode.getCounterNumber())
          self.__barCodeLabel = myLabel(self,self.__row,self.__col)
          self.__barCodeLabel.setFontAll('arial',12,'bold')
          self.__barCodeLabel.setWidth(55)
          self.__barCodeLabel.setText('Counter: '+self.__barCodeNumber)
          self.__barCodeLabel.makeLabel()
          self.__barCodeLabel.grid(row=self.__row,column=self.__col,columnspan=3,sticky=W)
          self.__row += 1
##
##  Start a test for each type of widget...
##
##                 Test a column of string entries
##                   Sets up a name, an entry field accepting a string, and an enter button to accept
          results = []
          self.__strName = ['Test 1','Test 2','Test 3']  # A list of test for entry box
          self.__strTest = []                            # This list contains the results to be used later
          for mmm in self.__strName:
               self.__tempStrTest=myStringEntry(self,self.__row,self.__col,self.__mySaveIt)  # define instance of myEntry object
               self.__tempStrTest.setEntryText(mmm)   # Set test name in form
               self.__tempStrTest.setButtonText('Enter '+mmm) # Set button name in button
               self.__tempStrTest.setLabelWidth(15)
               self.__tempStrTest.setEntryWidth(10)
               self.__tempStrTest.setButtonWidth(20)
               self.__tempStrTest.makeEntry()          # call method to setup name, entry and button
               self.__tempStrTest.grid(row=self.__row,column=self.__col,sticky=W)
               self.__row += 1
               self.__strTest.append(self.__tempStrTest)
          if (cmjDiag >= allDiag or cmjDiag == 100): print 'XXXX packWindow::__init__:: after entries'
##		Add a column of integer input...
##                   Sets up a name, an entry field accepting an integer, and an enter button to accept
          self.__integerTest = []                   # This list contains the results to be used later
          self.__intName = ['block','plane','module_position']   # a list of integer values
          for mmm in self.__intName:
               self.__tempIntTest = myIntEntry(self,self.__row,self.__col,self.__mySaveIt)    # define an instant of a integer entry
               self.__tempIntTest.setEntryText(mmm)
               self.__tempIntTest.setButtonText('Enter '+mmm)
               self.__tempIntTest.setLabelWidth(15)
               self.__tempIntTest.setEntryWidth(10)
               self.__tempIntTest.setButtonWidth(20)
               self.__tempIntTest.makeEntry()
               self.__tempIntTest.grid(row=self.__row,column=self.__col)
               self.__integerTest.append(self.__tempIntTest)
               self.__row += 1
##		Add a column of floating point input...
##                   Sets up a name, an entry field accepting a floating, and an enter button to accept
          self.__floatName = ['Float Test 1','Float Test 2','Float Test 3']  # A list of float values
          self.__floatTest = []                  # This list contains the results to be used later
          for mmm in self.__floatName:
               self.__tempFloatTest = myFloatEntry(self,self.__row,self.__col,self.__mySaveIt)     # define an instant of a integer entry
               self.__tempFloatTest.setEntryText(mmm)
               self.__tempFloatTest.setButtonText('Enter '+mmm)
               self.__tempFloatTest.setLabelWidth(15)
               self.__tempFloatTest.setEntryWidth(10)
               self.__tempFloatTest.setButtonWidth(20)
               self.__tempFloatTest.makeEntry()
               self.__tempFloatTest.grid(row=self.__row,column=self.__col,sticky=W)
               self.__floatTest.append(self.__tempFloatTest)
               self.__row += 1
##    Define a row of check boxes
##            Check boxes have multiple (or single) box to check or uncheck, and a "enter" button
##              press the "enter" button to log the contents
          self.__checkTestSet = {         # define dictionary that contains the list of test
               'Test1':0,
               'Test2':0,
               'Test3':0,
               'Test4':0
               }
          self.__horizontalCheckBox = myRowCheck(self,self.__mySaveIt)
          self.__horizontalCheckBox.initializeCheckList(self.__checkTestSet)
          self.__horizontalCheckBox.setText('Check Box Group 1')
          self.__horizontalCheckBox.makeChecks()
          self.__horizontalCheckBox.grid(row=self.__row,column=self.__col,sticky=W)
          self.__row += 1
          self.__col = 0
##
##    Define a row of radio buttons
##          Radio buttons only have one state "turned" on.  Clicking multiple buttons
##          will result in the last button checked.
          self.__radioTestSet = {         # define dictionary that contains the list of test
               'Test 1':0,
               'Test 2':0,
               'Test 3':0,
               'Test 4':0
               }		
          self.__horizontalRadioBox = myRowRadio(self,self.__mySaveIt,self.__row,self.__col)
          self.__horizontalRadioBox.initializeRadioList(self.__radioTestSet)
          self.__horizontalRadioBox.setText('Radio Button Group 1')
          self.__horizontalRadioBox.makeRadios()
          self.__horizontalRadioBox.grid(row=self.__row,column=self.__col,sticky=W)
          self.__row += 1
#             A column of check boxes
          self.__oldRow = self.__row
          self.__row = 2
          self.__col += 1
          self.__columnCheckTestSet = {'TTest 1' : 0,
               'TTest 2': 0,
               'TTest 3': 0,
               'TTest 4':0
               }
          self.__verticalCheckBox = myColumnCheck(self,self.__mySaveIt,self.__row,self.__col)
          self.__verticalCheckBox.initializeCheckList(self.__columnCheckTestSet)
          self.__verticalCheckBox.setText('Check Box Group 2')
          self.__verticalCheckBox.makeChecks()
          self.__verticalCheckBox.grid(row=self.__row,column=self.__col,rowspan=len(self.__columnCheckTestSet))
          if (cmjDiag >= allDiag or cmjDiag == 100): print 'XXXX packWindow.__init__ verticalCheckBox self.__row = %d self.__col = %d' %(self.__row,self.__col)
          self.__row += 1
#             A column of radio buttons
          self.__row += len(self.__columnCheckTestSet)
          self.__radioTestSet = {         # define dictionary that contains the list of test
               'TTest 1':0,
               'TTest 2':0,
               'TTest 3':0,
               'TTest 4':0
               }
          self.__verticalRadioBox = myColumnRadio(self,self.__mySaveIt,self.__row,self.__col)
          self.__verticalRadioBox.initializeRadioList(self.__radioTestSet)
          self.__verticalRadioBox.setText('Radio Button Group 2')
          self.__verticalRadioBox.makeRadios()
          self.__verticalRadioBox.grid(row=self.__row,column=self.__col,rowspan=len(self.__columnCheckTestSet))
          if (cmjDiag >= allDiag or cmjDiag == 100): print 'XXXX packWindow.__init__ verticalRadioBox self.__row = %d self.__col = %d' %(self.__row,self.__col)
##
##            Start the entries for the second column
##
##		Display the mu2e logo in the upper right hand corner
          self.__row1 = 0
          self.__col += 1
          self.__logo = mu2eLogo(self,self.__row1,self.__col)     # display Mu2e logo!
          self.__logo.grid(row=self.__row1,column=self.__col,rowspan=3,sticky=NE)
          self.__row1 += 3
#         Display the date the script is being run
          self.__date = myDate(self,self.__row1,self.__col,10)      # make entry to row... pack right
          self.__date.grid(row=self.__row1,column=self.__col,sticky=E)
          self.__row1 += 1
#         Display the script's version number
          self.__version = myLabel(self,self.__row1,self.__col)
          self.__version.setForgroundColor('blue')
          self.__version.setFontAll('Arial',10,'bold')
          self.__version.setWidth(15)
          self.__version.setText(Version)
          self.__version.makeLabel()
          self.__version.grid(row=self.__row1,column=self.__col,stick=E)
##              Add status bar above the quit button and next counter button
          self.__statusBar = myLabel(self,self.__oldRow+1,0)
          self.__statusBar.setWidth(45)
          self.__statusBar.setText('')
          self.__statusBar.makeLabel()
          self.__statusBar.grid(row=self.__oldRow+1,column=1,columnspan=2)
#		Add the control buttons at the bottom of the GUI
#		Add the quit button
          self.__quitNow = Quitter(self,self.__oldRow+2,0)
          self.__quitNow.grid(row=self.__oldRow+2,column=0,sticky=W)
          if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0): print 'XXXX packWindow::__init__:: before next button(E)'
          self.__next = Button(self,text='testDataBase',command=self.nextCounter,bg='green',fg='black')
          self.__next.grid(row=self.__oldRow+2,column=self.__col,sticky=E)
          if (cmjDiag >= allDiag or cmjDiag == 100): print 'XXXX packWindow__init__::... myRow = %d' % (self.__row)
####
     def nextCounter(self):               ## Write results to data base, clear and ready for next counter
##        Save current counter's results.....
          self.__mySaveIt.saveBarCode(self.__barCode.getBarCode())
          self.__mySaveIt.saveCounterNumber(self.__barCode.getCounterNumber())
          self.__mySaveIt.saveResults()    ## write result to file for now... json file later
          if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0): 
               print 'XXXX packWindow::nextCounter... Test Complete for Counter'
          password = ""
          url = "https://dbweb3.fnal.gov:8443/hardwaredb/mu2edev/HdbHandler.py/loader"
          table = "hardware_position"
          user = self.__mySaveIt.getOperator()+" (SLF6.6:GUI)"
          if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0): print "XXXX packWindow::nextCounter... testDataBase:Operator = %s" % user
#		Temporarily remove updating the data base
#          myDataLoader = DataLoader(password,url,table,user)     ## Load dictionary
#          myDataLoader.addRow(self.buildRowString())             ## Write to data base
#          retVal,code,text = myDataLoader.send()                 ## Report transmission results....
#          if retVal:
#               self.__statusBar.setForgroundColor('black')
#               self.__statusBar.setFontAll('arial',10,'normal')
#               self.__statusBar.setText("Data Sent: "+text)
#               self.__statusBar.makeLabel()
#               if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0):
#                 print "XXXX __packSmallWindowGrid__::Next Counter: Data Trasmission Success!!!"
#                 print text
#          else:
#               self.__statusBar.setForgroundColor('red')
#               self.__statusBar.setFontAll('arial',10,'normal')
#               self.__statusBar.setText('Error: '+code+' '+text)
#               self.__statusBar.makeLabel()
#               if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0):
#                  print "XXXX __packSmallWindowGrid__::Next Counter: Failed!!!"
#                  print code
#                  print text
#         
##
##		Reset the fields and values in the script
          for mmm in self.__strTest:
               mmm.resetEntry()                # reset the string fields
          for mmm in self.__integerTest:
               mmm.resetEntry()               # reset the integer fields
          for mmm in self.__floatTest:
               mmm.resetEntry()               # reset the floating point fields
          self.__horizontalCheckBox.reset()   # reset the horizontal checkboxes
          self.__verticalCheckBox.reset()     # reset the vertical checkboxes
          self.__horizontalRadioBox.reset()   # reset the horizontal radio buttons
          self.__verticalRadioBox.reset()     # reset the vertical radio buttons
          self.__mySaveIt.reset()             # reset the output stream....
          self.__barCode.readBarCodeReader()  # get new barcode for next counter
          self.__barCodeLabel.setText('Counter: '+self.__barCode.getBarCode())  # Write new barcode number
##
     def buildRowString(self):                ## build a dictionary to be sent to the database
          self.__sendRow = {}                 ## define empty dictionary... Then update it below.
          self.__sendRow['hw_id'] = str(self.__barCode.getBarCode())
          self.__sendRow['detector'] = 'NearDet'
          self.__sendRow['hw_type'] = 'FEB'
          self.__sendRow['install_date'] = strftime('%Y-%m-%d %H:%M:%S')
#
          self.__tempDictionaryList = []
          self.__tempDictionaryList.append(self.__mySaveIt.getIntEntry(0))
          self.__tempDictionaryList.append(self.__mySaveIt.getIntEntry(1))
          self.__tempDictionaryList.append(self.__mySaveIt.getIntEntry(2))
#
          self.__tempDictionary = {}
          for mlist in self.__tempDictionaryList:
               for key in mlist.keys():
                 self.__tempDictionary[key] = mlist[key]
          for key1 in self.__tempDictionary.keys():
               self.__sendRow[key1] = self.__tempDictionary[key1]
          self.__sendRow['dcm_name'] = 'Buddy'    ## I don't know what this is
          self.__sendRow['dcm_port'] = 6432  ## I don't know what this is...
          self.__sendRow['worker'] = 'USA'
          for key in self.__sendRow.keys():
                if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0): print 'XXXX packWindow::buildRowString... sendRow... key = %s value = %s \n' % (key,self.__sendRow[key])
#          return(self.__sendRow)
          return {'hw_id' : self.__sendRow['hw_id'],
            'detector' : self.__sendRow['detector'],
            'hw_type' : self.__sendRow['hw_type'],
            'install_date' : self.__sendRow['install_date'],
            'block' : self.__sendRow['block'],
            'plane' : self.__sendRow['plane'],
            'module_position' : self.__sendRow['module_position'],
            'dcm_name' :self.__sendRow['dcm_name'],
            'dcm_port' : self.__sendRow['dcm_port'],
            'worker' : self.__sendRow['worker'],
            }
##
##
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################     
#######################################################################################
#######################################################################################
##
##	This class is the main class that sets up the GUI
##      window.  The User enters the various quantities:
##	string values, integer values, float values, check boxex and radio boxes
##
##      This particular version just checks the existing data base... so only
##        string and integer entries are tested, then written to the data base.
##
##	A "next counter" method button is setup... When clicked this button
##	  will cause the entered data to be sent to the data base.
##        Then all fields are cleared and ready for the next counter or
##        Device test results to be entered.
##     This class contains the functions needed to build the
##        strings to send the data to the database.  This class uses the 
##        client class (DataLoader) developed by the Fermilab data base group 
##        (Steven White, etc.)
##
##	Later input from USB devices (such as the barcode reader) will
##        read out in this class to be entered in the database when
##        the 'next counter" button is clicked.
##
##	This class is intended to be run from a separate program to test writing
##        string and integer data to the database.  To run this file, construct a
##        separate file that loads this module (myEntryPointModule.py):
##                           from Tkinter import *         # get widget class
##                           from cmjGuiLibGrid2015Mar3 import *
##                           from DataLoader import DataLoader
##                           import time
##                           if __name__ == '__main__':
##                           root = Tk()              # or Toplevel()
##                           bannerText = 'Mu2e         CRV Scintillation Counter Test'
##                           root.title(bannerText+Version)  
##                           myWindow=packSmallWindowGrid(root,0,0).grid()
##                           root.mainloop()
##     To run the module:
##                           $python myEntryPointModule.py
##
##      A second class (packSmallWindow) is provided later to test the database
##        writing as currently setup.
##	Convert from the packing geometry to the grid Geometry by cmj 2014Mar3
##.
class packSmallWindowGrid(Frame):
     def __init__(self,parent=NONE,myRow=0,myCol=0):
          Frame.__init__(self,parent)
          self.__row = myRow
          self.__col = myCol
          if (cmjDiag >= allDiag or cmjDiag == 100): print 'XXXX __packSmallWindowGrid__init__:: Enter driving method: packSmallWindowGrid '
          self.__mySaveIt = saveResult()
          self.__mySaveIt.openFile()
#            Get the operator name from a modal window...
##           These labels span multiple columns
##             To span multiple columns, columnspan in grid call must be set, then
##             the label width must be set to match the spacing
          self.__operator=Operator(self)  # include in row
          self.__operatorName = self.__operator.getLabel()
          self.__mySaveIt.saveOperator(self.__operatorName)
          self.__operatorLabel = myLabel(self,self.__row,self.__col)
          self.__operatorLabel.setForgroundColor('black')
          self.__operatorLabel.setText(self.__operatorName)
          self.__operatorLabel.setFontAll('Arial',12,'bold')
          self.__operatorLabel.setWidth(45)
          self.__operatorLabel.makeLabel()
          self.__operatorLabel.grid(row=self.__row,column=self.__col,sticky=W)
          self.__row += 1
##        Get the Bar Code number (therefore the counter number)
##            Later on use the barcode reader... this involves a USB reader
          self.__barCode = BarCode()
          self.__barCode.initializeBarCodeReader()
          self.__barCode.readBarCodeReader()
          self.__barCodeNumber = self.__barCode.getBarCode()
          self.__mySaveIt.saveBarCode(self.__barCodeNumber)
          self.__mySaveIt.saveCounterNumber(self.__barCode.getCounterNumber())
          self.__barCodeLabel = myLabel(self,self.__row,self.__col)
          self.__barCodeLabel.setFontAll('arial',12,'bold')
          self.__barCodeLabel.setWidth(45)
          self.__barCodeLabel.setText('Counter: '+self.__barCodeNumber)
          self.__barCodeLabel.makeLabel()
          self.__barCodeLabel.grid(row=self.__row,column=self.__col,sticky=W)
          self.__row += 1
          results = []
##                 Test a column of string entries
##                   Sets up a name, an entry field accepting a string, and an enter button to accept
          self.__strTest = []                     # This list contains the results to be used later
          self.__strName = ['dcm_name','worker']  # a list of test for entry box
          for mmm in self.__strName:
               self.__tempStrTest=myStringEntry(self,self.__row,self.__col,self.__mySaveIt)  # define instance of myEntry object
               self.__tempStrTest.setEntryText(mmm)   # Set test name in form
               self.__tempStrTest.setButtonText('Enter '+mmm) # Set button name in button
               self.__tempStrTest.setLabelWidth(15)
               self.__tempStrTest.setEntryWidth(10)
               self.__tempStrTest.setButtonWidth(20)
               self.__tempStrTest.makeEntry()          # call method to setup name, entry and button
               self.__tempStrTest.grid(row=self.__row,column=self.__col,sticky=W)
               self.__row += 1
               self.__strTest.append(self.__tempStrTest)
          if (cmjDiag >= allDiag or cmjDiag == 100): print 'XXXX packWindow::__init__:: after entries'
#		Add the test integer input...
          self.__integerTest = []                               # This list contains the results to be used later
          self.__intName = ['block','plane','module_position']  # a list of integer values
          for mmm in self.__intName:
               self.__tempIntTest = myIntEntry(self,self.__row,self.__col,self.__mySaveIt)    # define an instant of a integer entry
               self.__tempIntTest.setEntryText(mmm)
               self.__tempIntTest.setButtonText('Enter '+mmm)
               self.__tempIntTest.setLabelWidth(15)
               self.__tempIntTest.setEntryWidth(5)
               self.__tempIntTest.setButtonWidth(20)
               self.__tempIntTest.makeEntry()
               self.__tempIntTest.grid(row=self.__row,column=self.__col)
               self.__row += 1
               self.__integerTest.append(self.__tempIntTest)
#		Start the entries for the next column
          self.__row1 = 0
          self.__col += 1
          self.__logo = mu2eLogo(self,self.__row1,self.__col)     # display Mu2e logo!
          self.__logo.grid(row=self.__row1,column=self.__col,rowspan=3,sticky=NE)
          self.__row1 += 3
#         Display the date the script is being run
          self.__date = myDate(self,self.__row1,self.__col,10)      # make entry to row... pack right
          self.__date.grid(row=self.__row1,column=self.__col,sticky=E)
          self.__row1 += 1
#         Display the script's version number
          self.__version = myLabel(self,self.__row1,self.__col)
          self.__version.setForgroundColor('blue')
          self.__version.setFontAll('Arial',10,'bold')
          self.__version.setWidth(15)
          self.__version.setText(Version)
          self.__version.makeLabel()
          self.__version.grid(row=self.__row1,column=self.__col,stick=E)
##              Add status bar above quit button and next counter button
          self.__statusBar = myLabel(self,self.__row,0)
          self.__statusBar.setWidth(45)
          self.__statusBar.setText('')
          self.__statusBar.makeLabel()
          self.__statusBar.grid(row=self.__row,column=0,columnspan=2)
          self.__row += 1
#		Add the control buttons at the bottom of the GUI
#		Add the quit button
          self.__quitNow = Quitter(self,self.__row,0)
          self.__quitNow.grid(row=self.__row,column=0,sticky=W)
#		Add the next counter button... click this to enter results into database..
          if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0): print 'XXXX __packSmallWindowGrid__:: before next button'
          self.__next = Button(self,text='testDataBase',command=self.nextCounter,bg='green',fg='black')
          self.__next.grid(row=self.__row,column=self.__col,sticky=E)
          if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0):
              print 'XXXX __packSmallWindowGrid__init__::... Total Rows: myRow = %d' % (self.__row)
              print 'XXXX __packSmallWindowGrid__init__::... Total Cols: myCol = %d' % (self.__col)
     def nextCounter(self):                ## Write results to data base, clear and ready for next counter
##        Save current counter's results.....
          self.__mySaveIt.saveBarCode(self.__barCode.getBarCode())
          self.__mySaveIt.saveCounterNumber(self.__barCode.getCounterNumber())
          self.__mySaveIt.saveResults()    ## write result to file for now... json file later
          if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0): 
               print 'XXXX __packSmallWindowGrid__::nextCounter... Test Complete for Counter'
          password = ""
          url = "https://dbweb4.fnal.gov:8443/hardwaredb/mu2edev/HdbHandler.py/loader"
          group = "Demo Tables"
          table = "Hardware_Position"
          myDataLoader = DataLoader(password,url,group,table)
          myDataLoader.addRow(self.buildRowString())
          retVal,code,text = myDataLoader.send()
          if retVal:
               self.__statusBar.setForgroundColor('black')
               self.__statusBar.setFontAll('arial',10,'normal')
               self.__statusBar.setText("Data Sent: "+text)
               self.__statusBar.makeLabel()
               if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0):
                 print "XXXX __packSmallWindowGrid__::Next Counter: Data Trasmission Success!!!"
                 print text
          else:
               self.__statusBar.setForgroundColor('red')
               self.__statusBar.setFontAll('arial',10,'normal')
               self.__statusBar.setText('Error: '+code+' '+text)
               self.__statusBar.makeLabel()
               if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0):
                  print "XXXX __packSmallWindowGrid__::Next Counter: Failed!!!"
                  print code
                  print text
          for tempStr in self.__strTest:
                tempStr.resetEntry()           # reset the string fields
          for tempInt in self.__integerTest:
                tempInt.resetEntry()           # reset the integer fields.
          self.__mySaveIt.reset()              # reset the output stream....
          self.__barCode.readBarCodeReader()   # get new barcode for next counter
          self.__barCodeLabel.setText('Counter: '+self.__barCode.getBarCode())  # Write new barcode number
##
     def buildRowString(self):                 ## build a dictionary to be sent to the database
          self.__sendRow = {}                  ## define empty dictionary... Then update it below.
          self.__sendRow['hw_id'] = str(self.__barCode.getBarCode())
          self.__sendRow['detector'] = 'NearDet'
          self.__sendRow['hw_type'] = 'FEB'
          self.__sendRow['install_date'] = strftime('%Y-%m-%d %H:%M:%Z')
#
          self.__tempDictionaryList = []
          self.__tempDictionaryList.append(self.__mySaveIt.getIntEntry(0))
          self.__tempDictionaryList.append(self.__mySaveIt.getIntEntry(1))
          self.__tempDictionaryList.append(self.__mySaveIt.getIntEntry(2))
#
          self.__tempDictionary = {}
          for mlist in self.__tempDictionaryList:
               for key in mlist.keys():
                    self.__tempDictionary[key] = mlist[key]
                    if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0): print '---> testDataBase::buildRowString... self.__mlist... key = %s ... value = %s \n' % (key,mlist[key])
          for key1 in self.__tempDictionary.keys():
               if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0): print '---> testDataBase::buildRowString... self.__tempDictionary... key1 = %s ... value = %s \n' % (key1,self.__tempDictionary[key1])
               self.__sendRow[key1] = self.__tempDictionary[key1]
          if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0): print 'XXXX __packSmallWindowGrid__::buildRowString self.__mySaveIt.getStrEntry(0) = %s' %(self.__mySaveIt.getStrEntry('dcm_name'))
          self.__sendRow['dcm_name'] = str(self.__mySaveIt.getStrEntry('dcm_name'))   ## I don't know what this is
          self.__sendRow['dcm_port'] = 6432  ## I don't know what this is...
          self.__sendRow['worker'] = str(self.__mySaveIt.getStrEntry('worker'))  ## I don't know what this is
          for key in self.__sendRow.keys():
               if (cmjDiag >= allDiag or cmjDiag == 9 or cmjDiagNext != 0): print 'XXXX __packSmallWindowGrid__::::buildRowString... sendRow... key = %s value = %s \n' % (key,self.__sendRow[key])

          return {'hw_id' : self.__sendRow['hw_id'],
            'detector' : self.__sendRow['detector'],
            'hw_type' : self.__sendRow['hw_type'],
            'install_date' : self.__sendRow['install_date'],
            'block' : self.__sendRow['block'],
            'plane' : self.__sendRow['plane'],
            'module_position' : self.__sendRow['module_position'],
            'dcm_name' :self.__sendRow['dcm_name'],
            'dcm_port' : self.__sendRow['dcm_port'],
            'worker' : self.__sendRow['worker'],
            }
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################
#######################################################################################     
#######################################################################################
#######################################################################################
## Entry point Method to test this module...
##   $python thisModulesName.py       
if __name__ == '__main__':
##
     root = Tk()                  # Top Level Window
     bannerText = 'Mu2e         CRV All Widgets'
     root.title(bannerText+Version)  
     myWindow=packWindow(root,0,0).grid()
     root.mainloop()               # run the GUI
     