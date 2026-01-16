# -*- coding: utf-8 -*-
##
##  File = "Modules.py"
##  Derived from File = "Modules_2017Jan13.py"
##  Derived from File = "Modules_2017Jan12.py"
##  Derived from File = "Modules_2016Dec20.py"
##  Derived from File = "DiCounters_2016Dec20.py"
##  Derived from File = "DiCounters_2016Dec19.py"
##  Derived from File = "DiCounters_2016Jun14.py"
##  Derived from File = "DiCounters_2016May16.py"
##  Derived from File = "Counters_2016May13.py"
##  Derived from File = "Fibers_2016May12.py"
##  Derived from File = "Extrusions_2016Jan27.py"
##  Derived from File = "Extrusions_2016Jan26.py"
##  Derived File = "Extrusions_2016Jan21.py"
##  Derived from File = "Extrusions_2016Jan14.py"
##  Derived from File = "Extrusions_2016Jan7.py"
##  Derived from File = "Extrusions_2015Oct12.py"
##
##  Test program to read in the lines from a comma separated
##  file saved from a spread sheet.  Here the delimiter is 
##  a tab and text is enclosed in "
##
##   Merrill Jenkins
##   Department of Physics
##   University of South Alabama
##   2015Sep23
##
#!/bin/env python
##
##  To run this script:
##      1) Input the initial Module information... 
##      python Modules.py -i 'moduleSpreadSheets/Module_2016Dec20.csv'
##      2) Input the test results
##      python Modules.py -i 'moduleSpreadSheets/ModuleTests_2017Jan13.csv' --mode 'measure'
##
##  Modified by cmj 2016Jan7... Add the databaseConfig class to get the URL for 
##            the various databases... change the URL in this class to change for all scripts.
##  Modified by cmj 2016Jan14 to use different directories for support modules...
##            These are located in zip files in the various subdirectories....
##  Modified by cmj2016Jan26.... change the maximum number of columns decoded to use variable.
##                        change code to accomodate two hole positions                                                            "pre_production" or "production"
##  Modified by cmj2016Jun24... Add one more upward level for subdirectory to get to the utilities directory
##  Modified by cmj2017Jan13... Add instructions for use in the call of the script.
##  Modified by cmj2017Feb2... Add test mode option; option to turn off send to database.
##  Modified by cmj2018Jun8... Change to hdbClient_v2_0
##  Modified by cmj2018Oct4.... Change the crvUtilities to contain version of cmjGuiLibGrid2018Oct1 that adds
##                        yellow highlight to selected scrolled list items
##  Modified by cmj2019May16... Change "hdbClient_v2_0" to "hdbClient_v2_2"
##  Modified by cmj2019May23... Add update mode for modules...
##  Modified by cmj2019May23... Add a loop to give maxTries to send information to database.
##  Modified by cmj2020May29... Make changes to read in new format for cvs file that contains Sipm Mounting Block
##                              information, the SipmId... These new cvs files are found with years of 2020 and later
##  Modified by cmj2020Jun8... Add code for the Steve's redesigned tables: diCounters -> counterMountingBoard -> sipmId
##  Modified by cmj2020Jun16... Change to cmjGuiLibGrid2019Jan30
##  Modified by cmj2020Jul02... Add "sleep" function to give database time to respond
##  Modifeid by cmj2020Jul22... Add "corner-case" modules....
##  Modified by cmj2020Jul22... Add a module to set the "sleepTime" variable.
##  Modified by cmj 2020Aug03 cmjGuiLibGrid2019Jan30 -> cmjGuiLibGrid
##  Modified by cmj2020Dec16... replace hdbClient_v2_2 with hdbClient_v3_3 - and (&) on query works
##
##  Modified by bt2021Feb15... implemented/corrected support for unstaggered module specifically typed "Downstream-short"
##  Modified by cmj2021May12... Include better comments to flag where BTuffs changed the code to impliment unstaggered csv files.
##
##  Modified by cmj2021Mar1.... Convert from python2 to python3: 2to3 -w *.py
##  Modified by cmj2021Mar1.... replace dataloader with dataloader3
##  Modified by cmj2021May11... replace tabs with spaces for block statements to convert to python 3##  
##
sendDataBase = 0  ## zero... don't send to database
#
import os
import sys        ## 
import optparse   ## parser module... to parse the command line arguments
import math
from collections import defaultdict
from time import *
sys.path.append("../../Utilities/hdbClient_v3_3/Dataloader.zip")  ## 2020Dec16
sys.path.append("../CrvUtilities/crvUtilities.zip")      ## 2018Oct2 add highlight to scrolled list, 2020 fix filename
from DataLoader import *   ## module to read/write to database....
from databaseConfig import *
from cmjGuiLibGrid import *       ## 2020Aug03
from generalUtilities import generalUtilities

ProgramName = "Modules.py"
Version = "version2021.05.12"


##############################################################################################
##############################################################################################
###  Class to store diCounter elements
class crvModules(object):
  def __init__(self):
    self.__cmjDebug = 10        ## no debug statements
    self.__sendToDatabase = 0  ## Do not send to database
    self.__maxTries = 2            ## set up maximum number of tries to send information to the database.
    self.__sleepTime = 0.5  ##  time interval between database requests
    self.__update = 0            ## update = 0, add new entry, update = 1, update existing entry.
    self.__database_config = databaseConfig()
    self.__url = ''
    self.__password = ''  ## for the Composite Tables
    self.__password2 = '' ## for the Sipms Table
    self.__password3 = '' ## for Electronics Tables
    self.__cmjDebug = 0   ## initialize without debugs
    ## List the corner case modules
    self.__moduleCornerCaseId = ['crvmod-101','crvmod-102','crvmod-103','crvmod-104','crvmod-105','crvmod-106','crvmod-107','crvmod-114','crvmod-115']
    self.__dummyCounter = 0 ## A dummy counter to give a unique id for the CmbId if it is a reflector or absorber
    ## Module Initial information
    self.__currentModuleId = ''            ## For the one module per spreadsheet scheme... store the module id
    self.__moduleId = ''            
    self.__moduleType = ''
    self.__moduleConstructionDate = ''
    self.__moduleLocation = ''
    self.__moduleWidth = ''
    self.__moduleLength = ''
    self.__moduleThick = ''
    self.__moduleEpoxyLot = ''
    self.__moduleAluminum  = ''
    self.__moduleDeviationFromFlat = ''
    self.__moduleComments = ''
    self.__moduleStagger = True # Contains whether module is staggered or not ... BTuffs 2021Feb15
    ##                              ## The layer and position are contained in the di-counter tables....
    self.__moduleDiCounterPosition = defaultdict(dict)  ## Nested dictionary to hold the position and layer of a 
                                          ## DiCounter in the module....
                                          ## (keys: [layer][position]
                                          ## layer ranges from top to bottom: layer1, layer2, layer3, layer4
                                          ## position 0, 1, 2, 3, 4, 5, 6, 7
    self.__moduleDiCounterId = defaultdict(dict)        ## Nested dictionary to hold the diCounerId at
                                          ## position and layer in a module 
                                          ## (keys: [layer][position]
                                          ## layer ranges from top to bottom: layer1, layer2, layer3, layer4
                                          ## position 0, 1, 2, 3, 4, 5, 6, 7
##
    self.__moduleDiCounterSipmId = defaultdict(dict)      ## Nested dictionary to hold the SipmId for a given di-counter
                                          ## (keys: [dicounterId][SipmPosition]
                                          ## the dicounterId is just that: diCounterId
                                          ## SipmPosition = a1, a2, a3, a4, b1, b2, b3, b4

## Di-Counters Initial information
    self.__startTime = strftime('%Y_%m_%d_%H_%M')
#    self.__sleepTime = 1.0
## -----------------------------------------------------------------
  def __del__(self):
    self.__stopTime = strftime('%Y_%m_%d_%H_%M')
    self.__endBanner= []
    self.__endBanner.append("## ----------------------------------------- \n")
    self.__endBanner.append("## Program "+ProgramName+" Terminating at time "+self.__stopTime+" \n")
    for self.__endBannerLine in self.__endBanner:
      self.__logFile.write(self.__endBannerLine)
## -----------------------------------------------------------------
  def turnOnDebug(self,tempDebug):
    self.__cmjDebug = tempDebug  # turn on debug
    print("...crvModules::turnOnDebug... turn on debug \n")
## -----------------------------------------------------------------
  def turnOffDebug(self):
    self.__cmjDebug = 0  # turn on debug
    #print("...crvModules::turnOffDebug... turn off debug \n")
## -----------------------------------------------------------------
  def turnOnSendToDatabase(self):
    self.__sendToDatabase = 1      ## send to database
    print(("...crvModules::turnOnSendToDataBase... send to database: self.__sendToDatabase = %s \n") % (self.__sendToDatabase))
## -----------------------------------------------------------------
  def turnOffSendToDatabase(self):
    self.__sendToDatabase = 0      ## send to database
    print("...crvModules::turnOffSendToDatabase... do not send to database \n")
## -----------------------------------------------------------------
  def sendToDevelopmentDatabase(self):
    self.__sendToDatabase = 1      ## send to database
    self.__whichDatabase = 'development'
    if(self.__cmjDebug > 9): self.__database_config.setDebugOn()
    print("...crvModules::sendToDevelopmentDatabase... send to development database \n")
    self.__url = self.__database_config.getWriteUrl()
    self.__password = self.__database_config.getCompositeKey()
    self.__password2 = self.__database_config.getSipmKey()
    self.__password3 = self.__database_config.getElectronicsKey()
    if(self.__cmjDebug > 9):
      print(("...crvModules::sendToDevelopmentDatabase... composite password  = xx%sxx") % self.__password)
      print(("...crvModules::sendToDevelopmentDatabase... sipm password       = xx%sxx") % self.__password2)
      print(("...crvModules::sendToDevelopmentDatabase... electronic password = xx%sxx") % self.__password3)
## -----------------------------------------------------------------
  def sendToProductionDatabase(self):
    self.__sendToDatabase = 1      ## send to database
    self.__whichDatabase = 'production'
    if(self.__cmjDebug > 9): self.__database_config.setDebugOn()
    print("...crvModules::sendToProductionDatabase... send to production database \n")
    self.__url = self.__database_config.getProductionWriteUrl()
    self.__password = self.__database_config.getCompositeProductionKey()
    self.__password2 = self.__database_config.getSipmProductionKey()
    self.__password3 = self.__database_config.getElectronicsProductionKey()
    if(self.__cmjDebug > 9):
      print(("...crvModules::sendToProductionDatabase... composite password  = xx%sxx") % self.__password)
      print(("...crvModules::sendToProductionDatabase... sipm password       = xx%sxx") % self.__password2)
      print(("...crvModules::sendToProductionDatabase... electronic password = xx%sxx") % self.__password3)
## ---------------------------------------------------------------
##  Change dataloader to update rather than insert.
  def updateMode(self):
    print("...crvModules::updateMode ==> change from insert to update mode")
    self.__update = 1
## -----------------------------------------------------------------
##  Allow the user to vary the sleep time 
  def setSleepyTime(self,tempSleepTime):
    self.__sleepTime = tempSleepTime
    print(("...crvModules::setSleepyTime ==> change sleep time to : %s \n") % (self.__sleepTime))
## -----------------------------------------------------------------
## -----------------------------------------------------------------
## -----------------------------------------------------------------
## -----------------------------------------------------------------
###############################################1###############################################
##############################################################################################
##############################################################################################
###   This is a class to read in an Excel spreadsheet page saved as a comma separated file
###   for the Sipms... The results are written in the database
###   The user can choose between the development or production database....
###
### -----------------------------------------------------------------
  def openFile(self,tempFileName):      ## method to open the file
    self.__inFileName = tempFileName
    self.__inFile=open(self.__inFileName,'r')   ## read only file

## -----------------------------------------------------------------
  def readFileSmbCmb(self,tempInputMode):            ## method to read the Smb/Cmb file's contents
    ## Module Test information
    self.__moduleTestDate = {}                  ## Dictionary to hold the date of the tests (key modulesId)
    self.__moduleTestLightSource = {}            ## Dictionary to hold the test light source (key modulesId)
    self.__moduleTestLightYieldAverage = {}      ## Dictionary to hold the test light average (key modulesId)
    self.__moduleTestLightYieldStDev = {}      ## Dictionary to hold the test light StDev (key modulesId)
    self.__moduleTestComments = {}            ## Dictionary to hold comments on the module (key modulesId)
    ## New 2020 Stuff
    self.__moduleSmbIdSideA = {}              ## Dictionary to hold the Simp Mounting Block Id... Side A
                                          ## (key: [dicounterId])
    self.__moduleSmbIdSideB = {}              ## Dictionary to hold the Sipm Mounting Block Id... Side B
                                          ## (key: [dicounterId])
    self.__moduleCmbIdSideA = {}        ## Dictionary to hold the Counter Mother Board Id... Side A
                              ## (keys: [dicounterId])
                              ## the diCounterId is just that: diCounterId
    self.__moduleCmbIdSideB = {}        ## Dictionary to hold the Counter Mother Board Id... Side B
                              ## (keys: [dicounterId])
                              ## the diCounterId is just that: diCounterId
    self.__moduleLayer_list = ['layer1','layer2','layer3','layer4']
    self.__moduleLayer_dict ={'Layer-1-A':'Layer-1-A', 'Layer-2-A':'Layer-2-A','Layer-3-A':'Layer-3-A','Layer-4-A':'Layer-4-A','Layer-1-B':'Layer-1-B', 'Layer-2-B':'Layer-2-B','Layer-3-B':'Layer-3-B','Layer-4-B':'Layer-4-B'}
    self.__diCounterSipmLocation_dict = {'A1':'A1','A2':'A2','A3':'A3','A4':'A4','B1':'B1','B2':'B2','B3':'B3','B4':'B4','S1':'S1','S2':'S2','S3':'S3','S4':'S4'}
    self.__diCounterSipms_list = ['A1','A2','A3','A4','B1','B2','B3','B4','S1','S2','S3','S4']
    self.__diCmbSipms_list = ['a1','a2','a3','a4','b1','b2','b3','b4']
    self.__moduleSmbToModuleLayer_dict = {'SMB-L1-A':'Layer-1-A', 'SMB-L2-A':'Layer-2-A','SMB-L3-A':'Layer-3-A','SMB-L4-A':'Layer-4-A', 'SMB-L1-B':'Layer-1-A', 'SMB-L2-B':'Layer-2-A','SMB-L3-B':'Layer-3-A','SMB-L4-B':'Layer-4-A'}
    self.__moduleCmbToModuleLayer_dict = {'CmbId-L1-A':'Layer-1-A', 'CmbId-L2-A':'Layer-2-A','CmbId-L3-A':'Layer-3-A','CmbId-L4-A':'Layer-4-A', 'CmbId-L1-B':'Layer-1-A', 'CmbId-L2-B':'Layer-2-A','CmbId-L3-B':'Layer-3-A','CmbId-L4-B':'Layer-4-A'}
    self.__moduleSipmToModuleLayer_dict = {'SipmId-L1-A':'Layer-1-A','SipmId-L2-A':'Layer-2-A','SipmId-L3-A':'Layer-3-A','SipmId-L4-A':'Layer-4-A','SipmId-L1-B':'Layer-1-B','SipmId-L2-B':'Layer-2-B','SipmId-L3-B':'Layer-3-B','SipmId-L4-B':'Layer-4-B'}
    self.__moduleSipmToModuleLayerOneSide_dict = {'SipmId-L1-A':'Layer-1-A','SipmId-L2-A':'Layer-2-A','SipmId-L3-A':'Layer-3-A','SipmId-L4-A':'Layer-4-A','SipmId-L1-B':'Layer-1-A','SipmId-L2-B':'Layer-2-A','SipmId-L3-B':'Layer-3-A','SipmId-L4-B':'Layer-4-A'}
##
    if(self.__cmjDebug > 0): print('mode value %s \n' % tempInputMode)
    self.__fileLine = []
    self.__fileLine = self.__inFile.readlines()  ## Read whole file here....
##      Sort, define and store information here...
##      For the new SMB csv files, read the file once to get the diCounter Ids stored in layer and position...
    if(tempInputMode == 'initial'):
      tempInputMode2='non-staggered'
      for self.__newLine in self.__fileLine:
        if(self.__cmjDebug > 8): print(("self.__newLine = %s \n") % (self.__newLine))
        if (self.__newLine.find('Module_Id') != -1):             
          self.storeModuleId(self.__newLine)
          #for nn in self.__moduleCornerCaseId:
          #  print("...crvModules::readFileSmbCmb__ nn = %s self.__moduleId = %s \n") % (nn, self.__moduleId)
          #  if (self.__moduleId== nn):
          #    tempInputMode2 = 'non-staggered'
          #    print("...crvModules::readFileSmbCmb__ READ STAGGERD INPUT FILE \n")
          #    break
        if (self.__newLine.find('Comments') != -1):             self.storeModuleComments(self.__newLine)
        if (self.__newLine.find('Layer') != -1 and self.__newLine.find('-A') != -1): 
          if(tempInputMode2 == 'staggered'):
            self.storeDicounterPosition(self.__newLine)
          else:
            self.storeDicounterPositionNonStaggered(self.__newLine)
##      Read the stored file information a second time to store the SMB, CMB and SipmId
      for self.__newLine2 in self.__fileLine:
        if(self.__cmjDebug > 10): print(("self.__newLine2 = %s \n") % (self.__newLine2))
        if(tempInputMode2== 'staggered'):
          if (self.__newLine2.find('SMB-') != -1):      self.storeSmb(self.__newLine2)
          if (self.__newLine2.find('CmbId-') != -1):      self.storeCmb(self.__newLine2)
          if (self.__newLine2.find('SipmId-') != -1):      self.storeDicounterSipmId(self.__newLine2)
        else:
          if (self.__newLine2.find('SMB-') != -1):      self.storeSmbNonStaggered(self.__newLine2)
          if (self.__newLine2.find('CmbId-') != -1):      self.storeCmbNonStaggered(self.__newLine2)
          if (self.__newLine2.find('SipmId-') != -1):      self.storeDicounterSipmIdNonStaggered(self.__newLine2)        
      print('Read in crvModules initial information')
    elif(tempInputMode == 'measure'):
      for self.__newLine in self.__fileLine:
        #print("self.__newLine = <%s>") % self.__newLine
        if (self.__newLine.find('crvModule-') != -1): self.storeModuleMeasure(self.__newLine)
      print('Read in crvModules test results information')
    print('end of crvModules::readFileSmbCmb')
    ## -----------------------------------------------------------------
    ## This method reads the older "layout csv files".   These files have been superceeded by the
    ## newer SBB files... This method has been retained with a different name for backwards
    ## compatibility.
  def readFileLayout(self,tempInputMode):            ## method to read the Layout file's contents
    ## Module Test information
    self.__moduleTestDate = {}                  ## Dictionary to hold the date of the tests (key modulesId)
    self.__moduleTestLightSource = {}            ## Dictionary to hold the test light source (key modulesId)
    self.__moduleTestLightYieldAverage = {}      ## Dictionary to hold the test light average (key modulesId)
    self.__moduleTestLightYieldStDev = {}      ## Dictionary to hold the test light StDev (key modulesId)
    self.__moduleTestComments = {}            ## Dictionary to hold comments on the module (key modulesId)

##
    if(self.__cmjDebug > 0): print('mode value %s \n' % tempInputMode)
    self.__fileLine = []
    self.__fileLine = self.__inFile.readlines()  ## Read whole file here....
##      Sort, define and store information here...
    if(tempInputMode == 'initial'):
      for self.__newLine in self.__fileLine:
        if(self.__cmjDebug > 0): print(("__readFileLayout__ self.__newLine = %s \n") % (self.__newLine))
        if (self.__newLine.find('Module_Id') != -1):             self.storeModuleId(self.__newLine)
        if (self.__newLine.find('Module_Type') != -1):            self.storeModuleType(self.__newLine)
        if (self.__newLine.find('Construction_Date') != -1):       self.storeModuleDate(self.__newLine)
        if (self.__newLine.find('Module_Location') != -1):       self.storeModuleLocation(self.__newLine)
        if (self.__newLine.find('Width_mm') != -1):             self.storeModuleWidth(self.__newLine)
        if (self.__newLine.find('Length_mm') != -1):             self.storeModuleLength(self.__newLine)
        if (self.__newLine.find('Thick_mm') != -1):            self.storeModuleThick(self.__newLine)
        if (self.__newLine.find('Epoxy_Lot') != -1):            self.storeModuleExpoxyLot(self.__newLine)
        if (self.__newLine.find('Aluminum') != -1):            self.storeModuleAluminum(self.__newLine)
        if (self.__newLine.find('Total_flatness_dev_mm') != -1): self.storeModuleFlat(self.__newLine)
        if (self.__newLine.find('Comments') != -1):             self.storeModuleComments(self.__newLine)
        if (self.__newLine.find('layer') != -1): 
        # BTuffs 2021Feb15.... Added if statements to check if module is staggered... begin
          if (self.__moduleStagger):
            self.storeDicounterPosition(self.__newLine)
          else:
            self.storeDicounterPositionNonStaggered(self.__newLine)
        # BTuffs 2021Feb15.... Added if statements to check if module is staggered... end
      print('Read in crvModules initial information')
    elif(tempInputMode == 'measure'):
      for self.__newLine in self.__fileLine:
        #print("self.__newLine = <%s>") % self.__newLine
        if (self.__newLine.find('crvModule-') != -1): self.storeModuleMeasure(self.__newLine)
      print('Read in crvModules test results information')
    print('end of crvModules::readFileLayout')
## -----------------------------------------------------------------
## -----------------------------------------------------------------
##      Methods to open logfiles
##    def setLogFileName(self,tempFileName):
##        self.__logFileName = 'logFiles/'+tempFileName+strftime('%Y_%m_%d_%H_%M')+'.txt'
## -----------------------------------------------------------------
  def openLogFile(self):
    self.__logFileName = 'logFiles/di-counter-logFile-'+strftime('%Y_%m_%d_%H_%M')+'.txt'
    self.__logFile = open(self.__logFileName,"w+")
    if(self.__cmjDebug == 2): print('----- saveResult::openFile: write to %s' % self.__logFileName)
    self.__banner = []
    self.__banner.append("##")
    self.__banner.append("##  module log file: "+self.__logFileName+"\n")
    self.__banner.append("##      This is a file that logs then di-counter entries into \n")
    self.__banner.append("##      the Mu2e CRV Quality Assurance/Quality Control Harware database \n")
    self.__banner.append("##  Program "+ProgramName+" Begining at time "+self.__startTime+" \n")
    self.__banner.append("##    Input Module File (.csv) = "+self.__inFileName+" \n")
    self.__banner.append("##    Start :"+self.__startTime+"\n")
    self.__banner.append("## \n")
    self.__banner.append("## ----------------------------------------- \n")
    self.__banner.append("## \n")
    for self.__beginBannerLine in self.__banner:
      self.__logFile.write(self.__beginBannerLine)
##
##
##      Method to setup access to the database
## -----------------------------------------------------------------
## -----------------------------------------------------------------
##
##      This method allows three different types of entries:
##      1) (inital) Setup the initial dicounter dimensions and history
##      2) (measure) Enter the test results... this may be done multiple times
##
## -----------------------------------------------------------------
## -----------------------------------------------------------------
##
  def sendToDatabase(self,tempInputMode):
    if(tempInputMode.strip() == 'initial'):
      self.connectDiCounterLayerPosition()
      self.sendModuleToDatabase()
      #self.connectDiCounterLayerPosition()
    elif(tempInputMode.strip() == 'measure'):
      self.sendModuleTestsToDatabase()
    else:
      print(("XXXX __crvModules__::sendToDatabase: invalid choice inputMode = %s") % tempInputMode) 
## -----------------------------------------------------------------
##  This is for the GUI... It can't pass arguments from button clicks!
  def sendLayoutToDatabase(self):
    self.sendModuleToDatabase()
    self.connectDiCounterLayerPosition()
    ## -----------------------------------------------------------------
##  This is for the GUI... It can't pass arguments from button clicks!
  def sendSmbCmbToDatabase(self):
    self.writeCounterMotherBoards()
    self.writeDiCounterSipms()
## -----------------------------------------------------------------
##  This is for the GUI... It can't pass arguments from button clicks!
  def sendTestToDatabase():
    self.sendModuleTestsToDatabase()
##
## -----------------------------------------------------------------
## -----------------------------------------------------------------
####  The next three functions construct the output string sent to the
####    database, send the string to the database and dump the string if needed...
####  Option 1: "initial": Send crvModules initial information to the database
####  Next send the crvModules data to the database... one crvModules at a time!
####  This done after the statistics for a batch have been loaded....
  def sendModuleToDatabase(self):
    self.__group = "Composite Tables"
    self.__crvModulesTable = "Modules"
    if(self.__cmjDebug > 0):
      print("XXXX __crvModules__::sendModuleToDatabase... self.__url = %s " % self.__url)
      print("XXXX __crvModules__::sendModuleToDatabase... self.__password = %s \n" % self.__password)
      ### Must load the crvModules table first!
    self.__crvModulesString = self.buildRowString_for_Module_table()
    self.logModuleString()
    if(self.__cmjDebug != 0) : 
      print(("XXXX __crvModules__::sendModuleToDatabase: self.__moduleId = %s") % (self.__moduleId))
      self.dumpModuleString()  ## debug.... dump crvModules string...
    if self.__sendToDatabase != 0:
      print("send crvModule to database!")
      self.__myDataLoader1 = DataLoader(self.__password,self.__url,self.__group,self.__crvModulesTable)
      if(self.__update == 0):                              ##cmj2019May23... add update
        self.__myDataLoader1.addRow(self.__crvModulesString,'insert')      ##cmj2019May23... add new line.. 2020Jul02 add "insert'
      else:
        self.__myDataLoader1.addRow(self.__crvModulesString,'update')  ## 2020Jul02
      for n in range(0,self.__maxTries):                        ## cmj2019May23... try to send maxTries time to database
        (self.__retVal,self.__code,self.__text) = self.__myDataLoader1.send()  ## send it to the data base!
        print("self.__text = %s" % self.__text)
        sleep(self.__sleepTime)     ## sleep so we don't send two records with the same timestamp....
        if self.__retVal:                        ## sucess!  data sent to database
          print("XXXX __crvModules__::sendModuleToDatabase: "+self.__moduleId+" Transmission Success!!!")
          self.__logFile.write('XXXX__diCounter__::sendModuleToDatabase: send '+self.__moduleId+' to database.')
          print(self.__text)
          break
        elif self.__password == '':
          print(('XXXX __crvModules__::sendModuleToDatabase: Test mode... DATA WILL NOT BE SENT TO THE DATABASE')())
          break
        else:
          print("XXXX __crvModules__::sendModuleToDatabase:  Counter Transmission: Failed!!!")
          print(self.__code)
          print(self.__text) 
          self.__logFile.write("XXXX__diCounter__::sendModuleToDatabase:  Transmission: Failed!!!")
          self.__logFile.write('XXXX__diCounter__::sendModuleToDatabase... self.__code = '+self.__code+'\n')
          self.__logFile.write('XXXX__diCounter__::sendModuleToDatabase... self.__text = '+self.__text+'\n')
    return 0
## -----------------------------------------------------------------
## -----------------------------------------------------------------
#### Build the string for a crvModules
  def buildRowString_for_Module_table(self):  
      self.__sendModuleRow = {}
      self.__sendModuleRow['module_id'] = self.__moduleId
      self.__sendModuleRow['module_type'] = self.__moduleType
      self.__sendModuleRow['location'] = self.__moduleLocation
      self.__sendModuleRow['width_mm'] = self.__moduleWidth
      self.__sendModuleRow['height_mm'] = self.__moduleLength
      self.__sendModuleRow['thick_mm'] = self.__moduleThick
      self.__sendModuleRow['expoxy_lot'] = self.__moduleEpoxyLot
      self.__sendModuleRow['aluminum'] = self.__moduleAluminum
      self.__sendModuleRow['deviation_from_flat_mm'] = self.__moduleDeviationFromFlat
      self.__sendModuleRow['comments'] = self.__moduleComments
      return self.__sendModuleRow
## ----------------------------------------------------------------- 
#### Diagnostic function to print out the dictionary for the fiber batch table:
  def dumpModuleString(self):
      print("XXXX __crvModules__::dumpModuleString:  Diagnostic")
      print("XXXX __crvModules__::dumpModuleString:  Print dictionary sent to database")
      for self.__tempLocal in self.__sendModuleRow:
        print(('    self.__sendModuleRow[%s] = %s') % (self.__tempLocal,str(self.__sendModuleRow[self.__tempLocal])))
## ----------------------------------------------------------------- 
#### Diagnostic function to print out the dictionary for the fiber batch table:
  def logModuleString(self):
    for self.__tempLocal in list(self.__crvModulesString.keys()):
      self.__logFile.write(' self.__crvModulesString['+self.__tempLocal+'] = '+str(self.__crvModulesString[self.__tempLocal])+'\n')
##
## -----------------------------------------------------------------
## -----------------------------------------------------------------
####  The next three functions construct the output string sent to the
####    database, send the string to the database and dump the string if needed...
####  Option 2: "measure": Send diCounter test results information to the database
####  Next send the diCounter data to the database... one crvModules at a time!
####  This done after the statistics for a batch have been loaded....
  def sendModuleTestsToDatabase(self):
    self.__group = "Composite Tables"
    self.__crvModulesTestsTable = "Module_Tests"
    if(self.__cmjDebug > 10):
      print("XXXX __crvModules__::sendModuleTestsToDatabase... self.__url = %s " % self.__url)
      print("XXXX __crvModules__::sendModuleTestsToRowoDatabase... self.__password = %s \n" % self.__password)
    for self.__localModuleTestsId in sorted(self.__moduleId.keys()):
      ### Must load the crvModules table first!
      self.__crvModulesTestsString = self.buildRowString_for_ModuleTests_table(self.__localModuleTestsId)
      self.logModuleString()
      if self.__cmjDebug != 0: 
        print(("XXXX __crvModules__::sendModuleTestsToDatabase: self.__localFiberId = %s") % (self.__localModuleTestsId))
        self.dumpModuleTestsString()  ## debug.... dump crvModules string...
      if self.__sendToDatabase != 0:
        if(self.__cmjDebug != 0): print("send to crvModules database!")
        self.__myDataLoader1 = DataLoader(self.__password,self.__url,self.__group,self.__crvModulesTestsTable)
        if(self.__update ==0):                                          ## cmj2019May23... add update
          self.__myDataLoader1.addRow(self.__crvModulesTestsString,'insert')      ## cmj2019May23... add new entry
        else:
          self.__myDataLoader1.addRow(self.__crvModulesTestsString,'update')      ## cmj2019May23... update existing row.
        for n in range(0,self.__maxTries):                              ## cmj2019May23... try to send to database maxTries times
          (self.__retVal,self.__code,self.__text) = self.__myDataLoader1.send()        ## send it to the data base!
          print("self.__text = %s" % self.__text)
          sleep(self.__sleepTime)     ## sleep so we don't send two records with the same timestamp....
          if self.__retVal:                        ## sucess!  data sent to database
            if(self.__cmjDebug !=0): print("XXXX __crvModules__::sendModuleTestsToDatabase:"+self.__moduleId[self.__localModuleTestsId]+" Transmission Success!!!")
            print(self.__text)
            break
          elif self.__password == '':
            print(('XXXX __crvModules__::sendModuleTestsToDatabase: Test mode... DATA WILL NOT BE SENT TO THE DATABASE')())
            break
          else:
            print("XXXX __crvModules__::sendModuleTestsToDatabase:  Counter Transmission: Failed!!!")
            print(self.__code)
            print(self.__text) 
            self.__logFile.write("XXXX__crvModules__::sendModuleTestsToDatabase:  Transmission: Failed!!!")
            self.__logFile.write('XXXX__crvModules__:sendModuleTestsToDatabase... self.__code = '+self.__code+'\n')
            self.__logFile.write('XXXX__crvModules__:sendModuleTestsToDatabase... self.__text = '+self.__text+'\n')
    return 0
## -----------------------------------------------------------------
## -----------------------------------------------------------------
#### Build the string for a crvModules tests...
  def buildRowString_for_ModuleTests_table(self,tempKey):  
    self.__sendModuleTestsToRow = {}
    self.__sendModuleTestsToRow['module_id'] = self.__moduleId[tempKey]
    self.__sendModuleTestsToRow['test_date'] = self.__moduleTestDate[tempKey]
    self.__sendModuleTestsToRow['light_yield_source'] = self.__moduleTestLightSource[tempKey]
    self.__sendModuleTestsToRow['light_yield_avg'] = self.__moduleTestLightYieldAverage[tempKey]
    self.__sendModuleTestsToRow['light_yield_stdev'] = self.__moduleTestLightYieldStDev[tempKey]
    self.__sendModuleTestsToRow['comments'] = self.__moduleTestComments[tempKey]
    return self.__sendModuleTestsToRow

## ----------------------------------------------------------------- 
#### Diagnostic function to print out the dictionary for the fiber batch table:
  def dumpModuleTestsString(self):
    print("XXXX __crvModules__::dumpModuleTestsString:  Diagnostic")
    print("XXXX __crvModules__::dumpModuleTestsString:  Print dictionary sent to database")
    for self.__tempLocal in self.__sendModuleTestsToRow:
      print(('    self.__sendModuleTestsToRow[%s] = %s') % (self.__tempLocal,str(self.__sendModuleTestsToRow[self.__tempLocal])))
## ----------------------------------------------------------------- 
#### Diagnostic function to print out the dictionary for the fiber batch table:
  def logModuleTestString(self):
    for self.__tempLocal in list(self.__crvModulesTestsString.keys()):
      self.__logFile.write(' self.__crvModulesTestsString['+self.__tempLocal+'] = '+str(self.__crvModulesTestsString[self.__tempLocal])+'\n')
##
## ***************************************************************************************
## ***************************************************************************************
##      Connect the layer and position of the dicounter
##      to the dicounter in the dicounter tables....
##      Note this is a different table!!!
  def connectDiCounterLayerPosition(self):
    self.__group = "Composite Tables"
    self.__diCounterTable = "Di_Counters"
    if(self.__cmjDebug != 0):  print("XXXX __crvModules__::writeDiCounterLayerPosition... self.__url = %s " % self.__url)
    if(self.__cmjDebug == 10): print("XXXX __crvModules__::writeDiCounterLayerPosition... self.__password = %s \n" % self.__password)
    ## loop over the di-counters.... layer, then position.....
    self.__localLayerIndex = 0
    for self.__localModuleLayer in sorted(self.__moduleDiCounterPosition.keys()):
      if(self.__cmjDebug > 1): print(("XXXX __crvModules__::writeDiCounterLayerPosition: localModuleLayer = %s \n") %(self.__localModuleLayer)) 
      self.__localDiCounterIndex = 0
      #for self.__localDiCounterPosition in sorted(self.__moduleDiCounterPosition[self.__localModuleLayer].keys()):
      for self.__localDiCounterPosition in range(0,8):
        self.__localDiCounterId = self.__moduleDiCounterId[self.__localModuleLayer][self.__localDiCounterPosition]
        if(self.__cmjDebug > 1): print(("XXXX __crvModules__::writeDiCounterLayerPosition: localDiCounterPosition = %s \n") %(self.__localDiCounterIndex)) 
        self.__diCounterString = self.buildDicounterLayerPositionString(self.__localDiCounterId,self.__localModuleLayer,self.__localDiCounterIndex)
        self.logDiCounterString()
        if self.__cmjDebug > 1: 
          print(("XXXX __crvModules__::writeDiCounterLayerPosition: self.__localDiCounterId = %s") % (self.__localDiCounterId))
          self.dumpDiCounterConnectionString()  ## debug.... dump diCounter string...
        if self.__sendToDatabase != 0:
          print("send to diCounter layer,position to  database!")
          self.__myDataLoader1 = DataLoader(self.__password,self.__url,self.__group,self.__diCounterTable)
          self.__myDataLoader1.addRow(self.__diCounterString,'update')  ## update the existing di-counter record
          for n in range(0,self.__maxTries):
            (self.__retVal,self.__code,self.__text) = self.__myDataLoader1.send()  ## send it to the data base!
            print("self.__text = %s" % self.__text)
            sleep(self.__sleepTime)     ## sleep so we don't send two records with the same timestamp....
            if self.__retVal:                        ## sucess!  data sent to database
              print("XXXX __crvModules__::writeDiCounterLayerPosition: diCounter:"+self.__localDiCounterId+' Layer: '+str(self.__localLayerIndex)+' Position: '+str(self.__localDiCounterIndex)+" Counter Transmission Success!!!")
              self.__logFile.write('XXXX __crvModules__::writeDiCounterLayerPosition: connect '+self.__localDiCounterId+' '+str(self.__localLayerIndex)+' '+str(self.__localDiCounterIndex)+' in database')
              print(self.__text)
              break
            elif self.__password == '':
              print(('XXXX__crvModules__::writeDiCounterLayerPosition: Test mode... DATA WILL NOT BE SENT TO THE DATABASE')())
              break
            else:
              print("XXXX__crvModules__::writeDiCounterLayerPosition:  Counter Transmission: Failed!!!")
              if(self.__cmjDebug > 1): 
                print("XXXX__crvModules__:writeDiCounterLayerPosition... Counter Transmission Failed: \n")
                print("XXXX__crvModules__:writeDiCounterLayerPosition... String sent to dataLoader: \n")
                print(("XXXX__crvModules__:writeDiCounterLayerPosition... self.__diCounterString \%s \n") % (self.__diCounterString))
              print(("XXXX__crvModules__:writeDiCounterLayerPosition... self.__code = %s \n") % (self.__code))
              print(("XXXX__crvModules__:writeDiCounterLayerPosition... self.__text = %s \n") % (self.__text)) 
              self.__logFile.write("XXXX__crvModules__::writeDiCounterLayerPosition:  Counter Transmission: Failed!!!")
              self.__logFile.write('XXXX__crvModules__::writeDiCounterLayerPosition... self.__code = '+self.__code+'\n')
              self.__logFile.write('XXXX__crvModules__::writeDiCounterLayerPosition... self.__text = '+self.__text+'\n')
        self.__localDiCounterIndex += 1
      self.__localLayerIndex += 1
## -----------------------------------------------------------------
## build the string to connect the dicounters in the layers and positions....
  def buildDicounterLayerPositionString(self,tempDiCounterId,tempModuleLayer,tempDiCounterPosition):
    self.__diCounterUpdate = {}
    self.__tempDiCounter = tempDiCounterId
    self.__diCounterUpdate['di_counter_id'] = tempDiCounterId
    self.__diCounterUpdate['module_id'] = self.__moduleId
    self.__diCounterUpdate['module_layer'] = str(tempModuleLayer)
    self.__diCounterUpdate['layer_position'] = str(tempDiCounterPosition)
    return self.__diCounterUpdate
## -----------------------------------------------------------------
## Diagnostic function to print out the dictionary for the dicounter connection string sent to database
  def dumpDiCounterConnectionString(self):
    print(("XXXX__crvModules__::dumpDiCounterConnectionString... self.__diCounterString = %s \n") %(self.__diCounterString))
    print(("XXXX__crvModules__::dumpDiCounterConnectionString... self.__diCounterUpdate['di_counter_id'] = %s") %(self.__diCounterUpdate['di_counter_id']))
    print(("XXXX__crvModules__::dumpDiCounterConnectionString... self.__diCounterUpdate['module_id'] = %s") %(self.__diCounterUpdate['module_id']))
    print(("XXXX__crvModules__::dumpDiCounterConnectionString... self.__diCounterUpdate['module_layer'] = %s") %(self.__diCounterUpdate['module_layer']))
    print(("XXXX__crvModules__::dumpDiCounterConnectionString... self.__diCounterUpdate['layer_position'] = %s \n") %(self.__diCounterUpdate['layer_position']))
    print(("XXXX__crvModules__::dumpDiCounterConnectionString... self.__diCounterUpdate['smb_id_a'] = %s") %(self.__diCounterUpdate['smb_id_a']))
    print(("XXXX__crvModules__::dumpDiCounterConnectionString... self.__diCounterUpdate['smb_id_b'] = %s") %(self.__diCounterUpdate['smb_id_b']))    
## -----------------------------------------------------------------
#### Diagnostic function to print out the dictionary for the fiber batch table:
  def logDiCounterString(self):
      for self.__tempLocal in list(self.__diCounterString.keys()):
        self.__logFile.write(' self.__diCounterString['+self.__tempLocal+'] = '+str(self.__diCounterString[self.__tempLocal])+'\n')
##
## 2020Jun8...... Save the Electronics Group, Cmb Table information
##
## ***************************************************************************************
## ***************************************************************************************
##      Enter the of the dicounter information into the 
##      the group: "Electronics Table", tables "Counter_Mother_Boards"
##      Note this is a different group and table!!!
##            Add cmb_id,di_counter_id, di_counter_end, smb_id
  def writeCounterMotherBoards(self):
    self.__group = "Electronic Tables"
    self.__diCounterTable = "counter_mother_boards"
    if(self.__cmjDebug != 0):  print("XXXX __crvModules__::writeCounterMotherBoards... self.__url = %s " % self.__url)
    if(self.__cmjDebug == 10): print("XXXX __crvModules__::writeCounterMotherBoards... self.__password = %s \n" % self.__password3)
    self.__localLayerIndex = 0
    for self.__localModuleLayerPosition in sorted(self.__moduleDiCounterPosition.keys()):  ##  loop over the Module Layers
      if(self.__cmjDebug > 1): print(("XXXX __crvModules__::writeCounterMotherBoards: self.__localModuleLayerPosition= %s \n") %(self.__localModuleLayerPosition)) 
      self.__localSipmIndex = 0
      self.__localDiCounterSide = 'top'
      for self.__localDiCounterIndex in sorted(self.__moduleDiCounterPosition[self.__localModuleLayerPosition].keys()):  ## loop over the diCounters in a layer
        self.__localSipmPosition = self.__diCounterSipms_list[self.__localSipmIndex]
        self.__localDiCounterId = self.__moduleDiCounterId[self.__localModuleLayerPosition][self.__localDiCounterIndex]
        self.__localDiCounterSide = 'top' 
        for self.__localSideIndex in range(0,2):      ## loop over the top and bottom
          if(self.__localSideIndex > 0): self.__localDiCounterSide = 'bottom' 
          #if(self.__cmjDebug > 0) : print("XXXX __crvModules__::writeCounterMotherBoards: self.__localSipmPosition = %s self.__localDiCounterId = %s \n") %(self.__localSipmPosition,self.__localDiCounterId) 
          if (self.__localDiCounterSide == 'top' ) : self._tempCmb = self.__moduleCmbIdSideA[self.__localDiCounterId]
          if (self.__localDiCounterSide == 'bottom' ) : self._tempCmb = self.__moduleCmbIdSideB[self.__localDiCounterId]
          if (self.__localDiCounterSide == 'top' ) : self._tempSmb = self.__moduleSmbIdSideA[self.__localDiCounterId]
          if (self.__localDiCounterSide == 'bottom' ) : self._tempSmb = self.__moduleSmbIdSideB[self.__localDiCounterId]
          if(self.__cmjDebug > 0) : print(("XXXX __crvModules__::writeCounterMotherBoards: DiCounterId = %s DiCounterSide = %s  cmbId = %s smbId = %s ") % (self.__localDiCounterId,self.__localDiCounterSide,self._tempCmb,self._tempSmb))
          self.__cmbSmbString = self.buildCmbSmbString(self.__localDiCounterId,self.__localSipmPosition,self.__localDiCounterSide)
          self.logCmbSmbString()
          if (self.__cmjDebug > 3): 
            print(("XXXX __crvModules__::writeCounterMotherBoards:(line637)  self.__localDiCounterId = %s self.__localDiCounterId = %s self.__localSipmPosition = %s") % (self.__localDiCounterId, self.__localDiCounterId,self.__localSipmPosition))
            self.dumpCmbSmbString()  ## debug.... dump diCounter string...
          if self.__sendToDatabase != 0:
            print("send to diCounter Cmb/Smb to database!")
            self.__myDataLoader1 = DataLoader(self.__password3,self.__url,self.__group,self.__diCounterTable)
            if(self.__update == 0):
              self.__myDataLoader1.addRow(self.__cmbSmbString,'insert')  ## insert new cmb record
            elif (self.__update == 1):
              self.__myDataLoader1.addRow(self.__cmbSmbString,'update')  ## update the existing cmb record
            for n in range(0,self.__maxTries):
              (self.__retVal,self.__code,self.__text) = self.__myDataLoader1.send()  ## send it to the data base!
              print("self.__text = %s" % self.__text)
              sleep(self.__sleepTime)     ## sleep so we don't send two records with the same timestamp....
              if self.__retVal:                        ## sucess!  data sent to database
                print("XXXX __crvModules__::writeCounterMotherBoards: diCounter:"+self.__localDiCounterId+' CmbId: '+self._tempCmb+' SmbId: '+self._tempSmb+" Cmb/Smb Transmission Success!!!")
                self.__logFile.write('XXXX __crvModules__::writeCounterMotherBoards: connect '+self.__localDiCounterId+' '+self.__localSipmPosition+' in database')
                print(self.__text)
                break
              elif self.__password == '':
                print(('XXXX__crvModules__::writeCounterMotherBoards: Test mode... DATA WILL NOT BE SENT TO THE DATABASE')())
                break
              else:
                print("XXXX__crvModules__::writeCounterMotherBoards:  Cmb/Smb Transmission: Failed!!!")
                print(("XXXX__crvModules__::writeCounterMotherBoards: CmbId = %s SmbId = %s") % (self._tempCmb,self._tempSmb))
                if(self.__cmjDebug > 1): 
                  print("XXXX__crvModules__:writeCounterMotherBoards... Counter Transmission Failed: \n")
                  print("XXXX__crvModules__:writeCounterMotherBoards... String sent to dataLoader: \n")
                  print(("XXXX__crvModules__:writeCounterMotherBoards... self.__localDiCounterId \%s \n") % (self.__localDiCounterId))
                print(("XXXX__crvModules__:writeCounterMotherBoards... self.__code = %s \n") % (self.__code))
                print(("XXXX__crvModules__:writeCounterMotherBoards... self.__text = %s \n") % (self.__text)) 
                self.__logFile.write("XXXX__crvModules__::writeCounterMotherBoards:  Counter Transmission: Failed!!!")
                self.__logFile.write('XXXX__crvModules__::writeCounterMotherBoards... self.__code = '+self.__code+'\n')
                self.__logFile.write('XXXX__crvModules__::writeCounterMotherBoards... self.__text = '+self.__text+'\n')
              if(self.__text.find(' already exists.') != -1) : break  ## If the member is already in the database, don't try maxTries times!
##   
## -----------------------------------------------------------------
## build the string to connect the dicounters to the Sipms....
  def buildCmbSmbString(self,tempDiCounterId ,tempSipmPosition, tempDiCounterSide):
    self.__cmbSmbUpdate = {}
    self.__cmbSmbUpdate['di_counter_id'] = tempDiCounterId
    self.__cmbSmbUpdate['di_counter_end'] = tempDiCounterSide
    if (tempDiCounterSide == 'top') : self.__cmbSmbUpdate['cmb_id'] = self.__moduleCmbIdSideA[tempDiCounterId]
    if (tempDiCounterSide == 'bottom') : self.__cmbSmbUpdate['cmb_id'] = self.__moduleCmbIdSideB[tempDiCounterId]
    if (tempDiCounterSide == 'top') : self.__cmbSmbUpdate['smb_id'] = self.__moduleSmbIdSideA[tempDiCounterId]
    if (tempDiCounterSide == 'bottom') : self.__cmbSmbUpdate['smb_id'] = self.__moduleSmbIdSideB[tempDiCounterId]
    return self.__cmbSmbUpdate
## -----------------------------------------------------------------
## Diagnostic function to print out the dictionary for the dicounter connection string sent to database
  def dumpCmbSmbString(self):
    print(("XXXX__crvModules__::self.__cmbSmbUpdate... self.__diCounterString = %s \n") %(self.__cmbSmbUpdate))
    #print("XXXX__crvModules__::self.__cmbSmbUpdate... self.__diCounterSipmUpdate['di_counter_id'] = %s") %(self.__cmbSmbUpdate['sipm_id'])
    print(("XXXX__crvModules__::self.__cmbSmbUpdate... self.__diCounterSipmUpdate['di_counter_id'] = %s") %(self.__cmbSmbUpdate['di_counter_id']))
    print(("XXXX__crvModules__::self.__cmbSmbUpdate... self.__diCounterSipmUpdate['di_counter_end'] = %s") %(self.__cmbSmbUpdate['di_counter_end']))
    print(("XXXX__crvModules__::self.__cmbSmbUpdate... self.__diCounterSipmUpdate['cmb_id'] = %s") %(self.__cmbSmbUpdate['cmb_id']))
    print(("XXXX__crvModules__::self.__cmbSmbUpdate... self.__diCounterSipmUpdate['smb_id'] = %s") %(self.__cmbSmbUpdate['smb_id']))    
## -----------------------------------------------------------------
#### Diagnostic function to print out the dictionary for the fiber batch table:
  def logCmbSmbString(self):
      for self.__tempLocal in list(self.__cmbSmbUpdate.keys()):
        self.__logFile.write(' self.__logCmbSmbString['+self.__tempLocal+'] = '+str(self.__cmbSmbUpdate[self.__tempLocal])+'\n')

## 2020Jun8...... Save the Electronics Group, Cmb Table information
## ----------------------------------------------------------------------------------------      
### 2020Jun4..... Save the Sipms Group, Sipms Table information
##
## ***************************************************************************************
## ***************************************************************************************
##      Enter the of the dicounter information into the 
##      the group: "Sipms Table", tables "Sipms"
##      Note this is a different group and table!!!
##            Only Add the cmb_id
  def writeDiCounterSipms(self):
    self.__group = "SiPM Tables"
    self.__diCounterTable = "sipms"
    if(self.__cmjDebug != 0):  print("XXXX __crvModules__::writeDiCounterSipms.. self.__url = %s " % self.__url)
    if(self.__cmjDebug == 10): print("XXXX __crvModules__::writeDiCounterSipms... self.__password = %s \n" % self.__password2)            
##      Loop over the layers, the the dicounter positions to get the diCounterId's            
    self.__localLayerIndex = 0
    self.__localDiCounterSide = 'a'
    for self.__localModuleLayerPosition in sorted(self.__moduleDiCounterPosition.keys()):      ## loop over the module layer
      if(self.__cmjDebug > 1): print(("XXXX __crvModules__::writeDiCounterSipms: self.__localModuleLayerPosition= %s \n") %(self.__localModuleLayerPosition)) 
      self.__localSipmIndex = 0
      for self.__localDiCounterIndex in sorted(self.__moduleDiCounterPosition[self.__localModuleLayerPosition].keys()):  ## loop over the Di-Counters in a layer.
        for self.__localSipmIndex in range(0,8):  ## loop over the Sipm Position inside a diCounter (both sides, a1, a2, a4, b1, b2, b3, b4)
          self.__localSipmPosition = self.__diCounterSipms_list[self.__localSipmIndex]
          self.__localDiCounterId = self.__moduleDiCounterId[self.__localModuleLayerPosition][self.__localDiCounterIndex]
          self.__localSipmId = self.__moduleDiCounterSipmId[self.__localDiCounterId][self.__localSipmPosition]
          self.__localCmbPosition = self.__diCmbSipms_list[self.__localSipmIndex] ## Give the Sipm location on the diCounter: a1, a2,a3,a4, b1,b2,b3,b4
          self.__localCmbId = self.__moduleCmbIdSideA[self.__localDiCounterId]
          if(self.__localSipmIndex > 3): self.__localCmbId = self.__moduleCmbIdSideB[self.__localDiCounterId]
          if(self.__cmjDebug > 10): print(("XXXX __crvModules__::writeDiCounterSipms:(line 726)  self.__localSipmId[%s][%s]= %s self.__localCmbId %s self.__localCmbPosition = %s\n") %(self.__localDiCounterId,self.__localSipmPosition,self.__localSipmId,self.__moduleDiCounterSipmId[self.__localDiCounterId][self.__localSipmPosition],self.__localCmbId,self.__localCmbPosition)) 
          self.__diCounterSipmString = self.buildDicounterSipmString(self.__localSipmId,self.__localCmbId,self.__localCmbPosition)
          self.logDiCounterSipmString()
          if (self.__cmjDebug > 1): 
            print(("XXXX __crvModules__::writeDiCounterSipms:  self.__localDiCounterId = %s self.__localCmbId = %s self.__localCmbPosition = %s") % (self.__localDiCounterId, self.__localCmbId,self.__localCmbPosition))
            self.dumpDiCounterSipmString()  ## debug.... dump diCounter string...
          if self.__sendToDatabase != 0:
            print("send to diCounter Cmb_Id and Cmb position to database!")
            self.__myDataLoader1 = DataLoader(self.__password2,self.__url,self.__group,self.__diCounterTable)
            self.__myDataLoader1.addRow(self.__diCounterSipmString,'update')  ## update the existing di-counter record
            for n in range(0,self.__maxTries):
              (self.__retVal,self.__code,self.__text) = self.__myDataLoader1.send()  ## send it to the data base!
              print("(Number of tries = %s) self.__text = %s" % (n,self.__text))
              if(n == 0): print(("XXXX __crvModules__::writeDiCounterSipms: self.__diCounterSipmString = %s") % (self.__diCounterSipmString))
              sleep(self.__sleepTime)     ## sleep so we don't send two records with the same timestamp....
              if self.__retVal:                        ## sucess!  data sent to database
                print('XXXX __crvModules__::writeDiCounterSipms: diCounterId: '+self.__localDiCounterId+' SipmId '+self.__localSipmId+' CmbId: '+self.__localCmbId+' CmbPosition '+self.__localCmbPosition+' Transmission Success!!!')
                self.__logFile.write('XXXX __crvModules__::writeDiCounterSipms:diCounterId: '+self.__localDiCounterId+' SipmId '+self.__localSipmId+' CmbId: '+self.__localCmbId+' CmbPosition '+self.__localCmbPosition+' in database')
                print(self.__text)
                break
              elif self.__password == '':
                print(('XXXX__crvModules__::writeDiCounterSipms: Test mode... DATA WILL NOT BE SENT TO THE DATABASE')())
                break
              else:
                print("XXXX__crvModules__::writeDiCounterSipms:  Counter Transmission: Failed!!!")
                if(self.__cmjDebug > 1): 
                  print("XXXX__crvModules__::writeDiCounterSipms... Counter Transmission Failed: \n")
                  print("XXXX__crvModules__::writeDiCounterSipms... String sent to dataLoader: \n")
                  print(("XXXX__crvModules__::writeDiCounterSipms... self.__localDiCounterId \%s \n") % (self.__localDiCounterId))
                print(("XXXX__crvModules__::writeDiCounterSipms... self.__code = %s \n") % (self.__code))
                print(("XXXX__crvModules__::writeDiCounterSipms... self.__text = %s \n") % (self.__text)) 
                self.__logFile.write("XXXX__crvModules__::writeDiCounterSipms:  Counter Transmission: Failed!!!")
                self.__logFile.write('XXXX__crvModules__::writeDiCounterSipms... self.__code = '+self.__code+'\n')
                self.__logFile.write('XXXX__crvModules__::writeDiCounterSipms... self.__text = '+self.__text+'\n')
##
##
## -----------------------------------------------------------------
## build the string to connect the dicounters to the Sipms....
  def buildDicounterSipmString(self,tempSipmId, tempCmbId ,tempCmbPosition):
    self.__diCounterSipmUpdate = {}
    self.__diCounterSipmUpdate['sipm_id'] = tempSipmId
    self.__diCounterSipmUpdate['cmb_id'] = tempCmbId
    self.__diCounterSipmUpdate['cmb_position'] = tempCmbPosition
    if(self.__cmjDebug > 5): print(("XXXX__crvModules__::buildDicounterSipmString: tempSipmId = %s tempCmbId = %s tempCmbPosition = %s ") % (tempSipmId,tempCmbId,tempCmbPosition))
    return self.__diCounterSipmUpdate
## -----------------------------------------------------------------
## Diagnostic function to print out the dictionary for the dicounter connection string sent to database
  def dumpDiCounterSipmString(self):
    print(("XXXX__crvModules__::dumpDiCounterSipmString... self.__diCounterString = %s \n") %(self.__diCounterSipmString))
    if(self.__diCounterSipmUpdate['sipm_id'].find('N/A') ==-1) : print(("XXXX__crvModules__::dumpDiCounterSipmString... self.__diCounterSipmUpdate['sipm_id'] = %s") %(self.__diCounterSipmUpdate['sipm_id']))
    print(("XXXX__crvModules__::dumpDiCounterSipmString... self.__diCounterSipmUpdate['cmb_id'] = %s") %(self.__diCounterSipmUpdate['cmb_id']))
    print(("XXXX__crvModules__::dumpDiCounterSipmString... self.__diCounterSipmUpdate['cmb_position'] = %s") %(self.__diCounterSipmUpdate['cmb_position']))  
## -----------------------------------------------------------------
#### Diagnostic function to print out the dictionary for the fiber batch table:
  def logDiCounterSipmString(self):
      for self.__tempLocal in list(self.__diCounterSipmString.keys()):
        self.__logFile.write(' self.__diCounterSipmString['+self.__tempLocal+'] = '+str(self.__diCounterSipmString[self.__tempLocal])+'\n')

### 2020Jun4..... Save the Sipms Group, Sipms Table information
# ----------------------------------------------------------------------------------------




##
## ***************************************************************************************
## ***************************************************************************************
##
##
##
## -----------------------------------------------------------------   
### Store functions.... must be called within the class to store the information
## -----------------------------------------------------------------
##
## -----------------------------------------------------------------
##  A series of functions for a single module per spreadsheet
  def storeModuleId(self,tempNewLine):
    self.__item= []
    self.__item = tempNewLine.rsplit(',')
    self.__moduleId = self.__item[1]
    if(self.__cmjDebug > 5) : 
      print(("__storeModuleId__ tempNewLine  = %s") % (tempNewLine))
      print(("__storeModuleId__ self.__item  = %s") % (self.__item))
      print(("__storeModuleId__ self.__moduleId  = %s") % (self.__moduleId))
  ## ----------------------------------
  def storeModuleType(self,tempNewLine):
    self.__item =[]
    self.__item = tempNewLine.rsplit(',')
    self.__moduleType = self.__item[1]
    if(self.__moduleType == ''): self.__moduleType = 'N/A'
    if(self.__cmjDebug > 5) : print(("__storeModuleType__ self.__moduleType = %s") % (self.__moduleType))
  ## ----------------------------------
  def storeModuleDate(self,tempNewLine):
    self.__item =[]
    self.__item = tempNewLine.rsplit(',')
    self.__moduleConstructionDate = self.__item[1]
    if(self.__moduleConstructionDate == ''): self.__moduleConstructionDate = 'N/A'
    if(self.__cmjDebug > 5) : print(("__storeModuleDate__ sself.__moduleConstructionDate = %s") % (self.__moduleConstructionDate))
  ## ----------------------------------
  def storeModuleLocation(self,tempNewLine):
    self.__item =[]
    self.__item = tempNewLine.rsplit(',')
    self.__moduleLocation = self.__item[1]
    if(self.__moduleLocation == ''): self.__moduleLocation = 'N/A'
    if(self.__cmjDebug > 0) : print(("__storeModuleLocation__ self.__moduleLocation = %s") % (self.__moduleLocation))
  ## ----------------------------------
  def storeModuleWidth(self,tempNewLine):
    self.__item =[]
    self.__item = tempNewLine.rsplit(',')
    self.__moduleWidth = self.__item[1]
    if(self.__moduleWidth == '') : self.__moduleWidth = -9999.99
    if(self.__cmjDebug > 5) : print(("__storeModuleWidth__ self.__moduleWidth = %s") % (self.__moduleWidth))
  ## ----------------------------------
  def storeModuleLength(self,tempNewLine):
    self.__item =[]
    self.__item = tempNewLine.rsplit(',')
    self.__moduleLength = self.__item[1]
    if(self.__moduleLength == ''): self.__moduleLength = -9999.99
    if(self.__cmjDebug > 0) : print(("__storeModuleLength__ self.__moduleLength = %s") % (self.__moduleLength))
  ## ----------------------------------
  def storeModuleThick(self,tempNewLine):
    self.__item =[]
    self.__item = tempNewLine.rsplit(',')
    self.__moduleThick = self.__item[1]
    if (self.__moduleThick == ''): self.__moduleThick = -9999.99
    if(self.__cmjDebug > 5) : print(("__storeModuleThick__ self.__moduleThick= %s") % (self.__moduleThick))
  ## ----------------------------------
  def storeModuleExpoxyLot(self,tempNewLine):
    self.__item =[]
    self.__item = tempNewLine.rsplit(',')
    self.__moduleEpoxyLot = self.__item[1]
    if(self.__moduleEpoxyLot == ''): self.__moduleEpoxyLot = 'N/A'
    if(self.__cmjDebug > 0) : print(("__storeModuleExpoxyLot__ self.__moduleExpoxyLot = %s") % (self.__moduleEpoxyLot)) 
  ## ----------------------------------
  def storeModuleAluminum(self,tempNewLine):
    self.__item =[]
    self.__item = tempNewLine.rsplit(',')
    self.__moduleAluminum = self.__item[1]
    if (self.__moduleAluminum == ''): self.__moduleAluminum = 'N/A'
    if(self.__cmjDebug > 5) : print(("__storeModuleAluminum__ self.__moduleAluminum = %s") % (self.__moduleAluminum))
  ## ----------------------------------
  def storeModuleFlat(self,tempNewLine):
    self.__item =[]
    self.__item = tempNewLine.rsplit(',')
    self.__moduleDeviationFromFlat = self.__item[1]
    if (self.__moduleDeviationFromFlat == ''): self.__moduleDeviationFromFlat = -9999.99
    if(self.__cmjDebug > 5) : print(("__storeModuleFlat__ self.__moduleDeviationFromFlat = %s") % (self.__moduleDeviationFromFlat))
  ## ----------------------------------
  def storeModuleComments(self,tempNewLine):
    self.__item =[]
    self.__words = [] # BTuffs 2021Feb15... to store comment sperated by spaces to test for stagger
    self.__item = tempNewLine.rsplit(',')
    self.__moduleComments = self.__item[1]
    self.__words = self.__moduleComments.rsplit(' ')
    #  BTuffs 2021Feb15... loop to check for 'non-staggered' in comments... begin
    for s in self.__words: #  BTuffs 2021Feb15... loop to check for 'non-staggered' in comments
      if(s == 'non-staggered'):
            self.__moduleStagger = False
    #  BTuffs 2021Feb15... loop to check for 'non-staggered' in comments... end
    if (self.__moduleComments == ''): self.__moduleComments = 'No comment!'
    if(self.__cmjDebug > 0) : print(("__ storeModuleComments__ self.__moduleComments = %s") % (self.__moduleComments))
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## ++++++++++++++++++++++++ End the CMB/SMB/SIPM Decode Files +++++++++++++++++++++++++++++++++++
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## =====================================================================================
## ========================== Set of functions to read the "staggered" files ===========
## =====================================================================================
## -----------------------------------------------------------------
##      Read the dicounter layer and position 
  def storeDicounterPosition(self,tempLayer):
    if(self.__cmjDebug > 0): print("__crvModules::storeDicounterPosition__ .... Enter \n")
    self.__item = [] 
    self.__item = tempLayer.rsplit(',')
    ## Read from the layout spreadsheets
    tempDiCounterPosition = 0
    if(self.__item[0] == 'layer1'):
      for n in range(5,13):
        self.__moduleDiCounterPosition[self.__item[0]][tempDiCounterPosition] = tempDiCounterPosition
        self.__moduleDiCounterId[self.__item[0]][tempDiCounterPosition] = 'di-'+self.__item[n].strip()
        tempDiCounterPosition += 1
    tempDiCounterPosition = 0
    if(self.__item[0] == 'layer2'):
      for n in range(4,12):
        self.__moduleDiCounterPosition[self.__item[0]][tempDiCounterPosition] = tempDiCounterPosition
        self.__moduleDiCounterId[self.__item[0]][tempDiCounterPosition] = 'di-'+self.__item[n].strip()
        tempDiCounterPosition += 1
    tempDiCounterPosition = 0
    if(self.__item[0] == 'layer3'):
      for n in range(3,11):
        self.__moduleDiCounterPosition[self.__item[0]][tempDiCounterPosition] = tempDiCounterPosition
        self.__moduleDiCounterId[self.__item[0]][tempDiCounterPosition] = 'di-'+self.__item[n].strip()
        tempDiCounterPosition += 1
    tempDiCounterPosition = 0
    if(self.__item[0] == 'layer4'):
      for n in range(2,10):
        self.__moduleDiCounterPosition[self.__item[0]][tempDiCounterPosition] = tempDiCounterPosition
        self.__moduleDiCounterId[self.__item[0]][tempDiCounterPosition] = 'di-'+self.__item[n].strip()
        tempDiCounterPosition += 1
    ##
    ##  Read the Dicounter Id's from the SMB spreadsheets.
    cellIncrement = 4
    if(self.__item[0] == 'Layer-1-A'):
      columnNumber = 14 ## start at offset to first cell
      for n in range(0,8):
        self.__moduleDiCounterPosition[self.__item[0]][n] = n
        self.__moduleDiCounterId[self.__item[0]][n] = 'di-'+self.__item[columnNumber].strip()
        columnNumber += cellIncrement
    if(self.__item[0] == 'Layer-2-A'):
      columnNumber = 10 ## start at offset to first cell
      for n in range(0,8):
        self.__moduleDiCounterPosition[self.__item[0]][n] = n
        self.__moduleDiCounterId[self.__item[0]][n] = 'di-'+self.__item[columnNumber].strip()
        columnNumber += cellIncrement
    if(self.__item[0] == 'Layer-3-A'):
      columnNumber = 6  ## start at offset to first cell
      for n in range(0,8):
        self.__moduleDiCounterPosition[self.__item[0]][n] = n
        self.__moduleDiCounterId[self.__item[0]][n] = 'di-'+self.__item[columnNumber].strip()
        columnNumber += cellIncrement
    if(self.__item[0] == 'Layer-4-A'):
      columnNumber = 2
      for n in range(0,8):
        self.__moduleDiCounterPosition[self.__item[0]][n] = n
        self.__moduleDiCounterId[self.__item[0]][n] = 'di-'+self.__item[columnNumber].strip()
        columnNumber += cellIncrement
    if(self.__cmjDebug > 6):  ## Diagnostic Print statements
      print("__crvModules::storeDicounterPosition__ : self.__moduleDiCounterPosition \n")
      tempLayer = self.__item[0]
      print("__crvModules::storeDicounterPosition__ ::: DiCounterPosition based upon [layer][postion]\n")
      for tempDiCounterPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
        print(("__crvModules::storeDicounterPosition__ self.__moduleDiCounterPosition[layer][position] = self.__moduleDiCounterPosition[%s][%s] = %s \n") % (tempLayer,tempDiCounterPosition,self.__moduleDiCounterPosition[tempLayer][tempDiCounterPosition]))
      print("__crvModules::storeDicounterPosition__  ----------------- \n")
      ##
      print("__crvModules::storeDicounterPosition__ ::: DiCounterId's based upon [layer][postion]\n")
      for tempDiCounterId in list(self.__moduleDiCounterPosition[tempLayer].keys()):
        print(("__crvModules::storeDicounterPosition__ self.__moduleDiCounterId[layer][position] =  self.__moduleDiCounterId[%s][%s] = %s \n") % (tempLayer,tempDiCounterId,self.__moduleDiCounterId[tempLayer][tempDiCounterId]))
      print("__crvModules::storeDicounterPosition__ .... Exit \n")
    
## -----------------------------------------------------------------
##  Store the Sipm Mother Board...
##  This function is assumes the DiCounter_id, layer and positions have
##  already been stored.
  def storeSmb(self, tempLine):
    self.__item = []
    self.__item = tempLine.rsplit(',')
    if(self.__cmjDebug > 0): print("__crvModules::storeSmb__ ... Enter \n")
    if(self.__cmjDebug > 0): print(("__crvModules::storeSmb__ self.__item[0] = Layer = %s \n") % (self.__item[0]))
    ##
    ##  Read Sipm Mother Board Ids' from the SMB spreadsheets.
    self.__tempLayer = 0
    cellIncrement = 4
    if(self.__item[0] == 'SMB-L1-A'):
      columnNumber = 14 ## start at offset to first cell
      self.__tempLayer = 'Layer-1-A'
      for n in range(0,8):      ## loop over all diCounters in a given layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-1-A'][n]
        tempSmbId = self.__item[columnNumber].strip()
        self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L2-A'):
      columnNumber = 10 ## start at offset to first cell
      self.__tempLayer = 'Layer-2-A'
      for n in range(0,8):      ## loop over all diCounters in a given layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-2-A'][n]
        tempSmbId = self.__item[columnNumber].strip()
        self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L3-A'):
      columnNumber = 6  ## start at offset to first cell
      self.__tempLayer = 'Layer-3-A'
      for n in range(0,8):      ## loop over all diCounters in a given layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-3-A'][n]
        tempSmbId = self.__item[columnNumber].strip()
        self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L4-A'):
      columnNumber = 2
      self.__tempLayer = 'Layer-4-A'
      for n in range(0,8):      ## loop over all diCounters in a given layer...
        tempDiCounterId = self.__moduleDiCounterId['Layer-4-A'][n]
        tempSmbId = self.__item[columnNumber].strip()
        self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        columnNumber += cellIncrement
    ## --- Other end of the module... side B
    ##
    if(self.__item[0] == 'SMB-L1-B'):
      columnNumber = 2 ## start at offset to first cell
      self.__tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempSmbId = self.__item[columnNumber].strip()
        self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L2-B'):
      columnNumber = 6 ## start at offset to first cell
      self.__tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempSmbId = self.__item[columnNumber].strip()
        self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L3-B'):
      columnNumber = 10  ## start at offset to first cell
      self.__tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempSmbId = self.__item[columnNumber].strip()
        self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L4-B'):
      columnNumber = 14
      self.__tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempSmbId = self.__item[columnNumber].strip()
        self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        columnNumber += cellIncrement
      ##
    if(self.__cmjDebug > 3):  ## Detail Diagnostic print statements
      if(self.__item[0].find('-A') != -1):
        print("__crvModules::storeSmb__ ... self.__moduleSmbIdSideA \n")
        tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempSmbId = self.__moduleSmbIdSideA[tempDiCounter]
          print(("__crvModules::storeSmb__   tempLayer = %s || tempPosition = %s || tempDiCounter = %s || self.__moduleSmbIdSideA = %s \n") % (tempLayer,tempPosition,self.__moduleDiCounterId[tempLayer][tempPosition],self.__moduleSmbIdSideA[tempDiCounter]))
      if(self.__item[0].find('-B') != -1):
        print("__crvModules::storeSmb__ ... self.__moduleSmbIdSideB \n")
        tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempSmbId = self.__moduleSmbIdSideB[tempDiCounter]
          print(("__crvModules::storeSmb__   tempLayer = %s || tempPosition = %s || tempDiCounter = %s || self.__moduleSmbIdSideB = %s \n") % (tempLayer,tempPosition,self.__moduleDiCounterId[tempLayer][tempPosition],self.__moduleSmbIdSideB[tempDiCounter]))
    ##
    if(self.__cmjDebug > 3):  ## Diagnostic print statements
      if(self.__item[0].find('-A') != -1):
        print("__crvModules::storeSmb__ ... self.__moduleSmbIdSideA \n")
        tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempSmbId = self.__moduleSmbIdSideA[tempDiCounter]
          print(("__crvModules::storeSmb__  self.__moduleSmbIdSideA[tempDiCounter] = self.__moduleSmbIdSideA[%s] = %s\n") % (tempDiCounter,self.__moduleSmbIdSideA[tempDiCounter]))
      if(self.__item[0].find('-B') != -1):
        print("__crvModules::storeSmb__ ... self.__moduleSmbIdSideB \n")
        tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempSmbId = self.__moduleSmbIdSideB[tempDiCounter]
          print(("__crvModules::storeSmb__  self.__moduleSmbIdSideA[tempDiCounter] = self.__moduleSmbIdSideB[%s] = %s\n") % (tempDiCounter,self.__moduleSmbIdSideB[tempDiCounter]))
      print("__crvModules::storeSmb__ ... Exit \n")
      
## -----------------------------------------------------------------
##  Store the Counter Mother Board...
##  This function is assumes the DiCounter_id, layer and positions have
##  already been stored.
  def storeCmb(self, tempLine):
    self.__item = []
    self.__item = tempLine.rsplit(',')
    if(self.__cmjDebug > 0): print("__crvModules::storeCmb__ ... Enter \n")
    if(self.__cmjDebug > 9): print(("__crvModules::storeCmb__ ... self.__item[0] = %s\n") % (self.__item[0]))
    ##  Read Counter Mother Board Ids' from the SMB spreadsheets.
    cellIncrement = 4
    if(self.__item[0] == 'CmbId-L1-A'):
      columnNumber = 14 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer...
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][n]
        tempCmbId = self.__item[columnNumber].strip()
        self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L2-A'):
      columnNumber = 10 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer...
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][n]
        tempCmbId = self.__item[columnNumber].strip()
        self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L3-A'):
      columnNumber = 6  ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer...
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][n]
        tempCmbId = self.__item[columnNumber].strip()
        self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L4-A'):
      columnNumber = 2
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer...
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][n]
        tempCmbId = self.__item[columnNumber].strip()
        self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        columnNumber += cellIncrement
    ## --- Other end of the module... side B
    ##
    if(self.__item[0] == 'CmbId-L1-B'):
      columnNumber = 2 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempCmbId = self.__item[columnNumber].strip()
        self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L2-B'):
      columnNumber = 6 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempCmbId = self.__item[columnNumber].strip()
        self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L3-B'):
      columnNumber = 10  ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempCmbId = self.__item[columnNumber].strip()
        self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L4-B'):
      columnNumber = 14
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempCmbId = self.__item[columnNumber].strip()
        self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        columnNumber += cellIncrement
    ##      Detailed Diagnostic print statements
    if(self.__cmjDebug > 3): 
      if(self.__item[0].find('-A') != -1):
        print("__crvModules::storeCmb__ ... self.__moduleCmbIdSideA \n")
        tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempCmbId = self.__moduleCmbIdSideA[tempDiCounter]
          print(("__crvModules::storeCmb__   tempLayer = %s || tempPosition = %s || tempDiCounter = %s || self.__moduleCmbIdSideA = %s \n") % (tempLayer,tempPosition,self.__moduleDiCounterId[tempLayer][tempPosition],self.__moduleCmbIdSideA[tempDiCounter]))
      if(self.__item[0].find('-B') != -1):
        print("__crvModules::storeCmb__ ... self.__moduleCmbIdSideB \n")
        tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempCmbId = self.__moduleCmbIdSideB[tempDiCounter]
          print(("__crvModules::storeCmb__   tempLayer = %s || tempPosition = %s || tempDiCounter = %s || self.__moduleCmbIdSideB = %s \n") % (tempLayer,tempPosition,self.__moduleDiCounterId[tempLayer][tempPosition],self.__moduleCmbIdSideB[tempDiCounter]))
##      Diagnostic Print statements
    if(self.__cmjDebug > 3): 
      if(self.__item[0].find('-A') != -1):
        print("__crvModules::storeCmb__ ... self.__moduleCmbIdSideA \n")
        tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempCmbId = self.__moduleCmbIdSideA[tempDiCounter]
          print(("__crvModules::storeCmb__   self.__moduleCmbIdSideA[diCounter] = self.__moduleCmbIdSideA[diCounter[%s] = %s \n") % (tempDiCounter,self.__moduleCmbIdSideA[tempDiCounter]))
      if(self.__item[0].find('-B') != -1):
        print("__crvModules::storeCmb__ ... self.__moduleCmbIdSideB \n")
        tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempCmbId = self.__moduleCmbIdSideB[tempDiCounter]
          print(("__crvModules::storeCmb__   self.__moduleCmbIdSideA[diCounter] = self.__moduleCmbIdSideB[diCounter[%s] = %s \n") % (tempDiCounter,self.__moduleCmbIdSideB[tempDiCounter]))
        print("__crvModules::storeCmb__ ... Exit \n")
##
## ----------------------------------------------------------------
##  Store the Sipm Id for Sipms on the end of dicounters... 
  def storeDicounterSipmId(self, tempLine):
    self.__item = []
    self.__item = tempLine.rsplit(',')
    self.__crvSipmBatch = "CrvSipm-S14283(ES2)_"
    if(self.__cmjDebug > 0): print("__crvModules::storeDicounterSipmId__ ... Enter xx \n")  
    ## --
    ##  Read Sipm Ids' from the SMB spreadsheets.
    diCounterCellIncrement = 4
    cellIncrement = 1
    if(self.__item[0] == 'SipmId-L1-A'):
      columnNumber = 14 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-1-A'][n]
        for m in range(0,4): ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L2-A'):
      columnNumber = 10 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-2-A'][n]
        for m in range(0,4):  ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L3-A'):
      columnNumber = 6  ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-3-A'][n]
        for m in range(0,4):
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L4-A'):
      columnNumber = 2
      for n in range(0,8):  ## Loop over all diCounter in this layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-4-A'][n]
        for m in range(0,4):  ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    ## --- Other end of the module... side B
    ##
    if(self.__item[0] == 'SipmId-L1-B'):
      columnNumber = 2 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer... This layer viewed on other end... use origional layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-1-A'][7-n]
        for m in range(0,4):  ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m+4]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L2-B'):
      columnNumber = 6 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer... This layer viewed on other end... use origional layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-2-A'][7-n]
        for m in range(0,4):  ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m+4]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L3-B'):
      columnNumber = 10  ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer... This layer viewed on other end... use origional layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-3-A'][7-n]
        for m in range(0,4):
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m+4]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L4-B'):
      columnNumber = 14
      for n in range(0,8):  ## Loop over all diCounter in this layer... This layer viewed on other end... use origional layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-4-A'][7-n]
        for m in range(0,4):  ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m+4]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    ## ------ Diagnostic output
    if(self.__cmjDebug > 4): 
      print("__crvModules::storeDicounterSipmId__ \n")
      mLayer = self.__moduleSipmToModuleLayer_dict[self.__item[0].strip()]
      nLayer = self.__moduleSipmToModuleLayerOneSide_dict[self.__item[0].strip()]
      print(("__crvModules::storeDicounterSipmId__ Layer = %s \n") %(nLayer))
      for mDiCounter in range(0,8): ## loop over all di-counters in a layer
        print(("---------> mLayer = %s || mDiCounter = %s \n") % (mLayer,mDiCounter))
        tempDiCounterId = self.__moduleDiCounterId[nLayer][mDiCounter]  ## find the diCounter Id
        print(("---------> mLayer = %s || mDiCounter = %s || tempDiCounterId = %s \n") % (mLayer,mDiCounter,tempDiCounterId))
        for tempSipmPosition in sorted(self.__diCounterSipmLocation_dict.keys()):  ## Find the SipmId for a diCounter: use [diCounterId][SipmPosition]
          if(tempSipmPosition.find('A') != -1 and mLayer.find('-A') != -1): print(("__crvModules::storeDicounterSipmId__ self.__moduleDiCounterSipmId[diCounterId][SipmPos] = self.__moduleDiCounterSipmId[%s][%s] = %s\n") % (tempDiCounterId,tempSipmPosition,self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition]))
          if(tempSipmPosition.find('B') != -1 and mLayer.find('-B') != -1): print(("__crvModules::storeDicounterSipmId__ self.__moduleDiCounterSipmId[diCounterId][SipmPos] = self.__moduleDiCounterSipmId[%s][%s] = %s\n") % (tempDiCounterId,tempSipmPosition,self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition]))
      print("__crvModules::storeDicounterSipmId__ ... Exit \n")
##
## =====================================================================================
## ========================== Set of functions to read the "non-staggered" files =======
## =====================================================================================
## -----------------------------------------------------------------
##      Read the dicounter layer and position 
##      From the cmb/smb spreadsheet....
  def storeDicounterPositionNonStaggered(self,tempLayer):
    if(self.__cmjDebug > 0): print("__crvModules::storeDicounterPositionNonStaggered__ .... Enter..... \n")
    self.__item = [] 
    self.__item = tempLayer.rsplit(',')
    if(self.__cmjDebug > 9): print(("__crvModules::storeDicounterPositionNonStaggered__ .... self.__item = %s \n") % (self.__item))
    ## Read from the layout spreadsheets
    tempDiCounterPosition = 0
    if(self.__item[0] == 'layer1'):
      #for n in range(5,13):
      for n in range(2,10): # BTuffs 2021Feb15... edited to be correct "non-stagger"
        self.__moduleDiCounterPosition[self.__item[0]][tempDiCounterPosition] = tempDiCounterPosition
        self.__moduleDiCounterId[self.__item[0]][tempDiCounterPosition] = 'di-'+self.__item[n].strip()
        tempDiCounterPosition += 1
    tempDiCounterPosition = 0
    if(self.__item[0] == 'layer2'):
      #for n in range(4,12):
      for n in range(2,10): # BTuffs 2021Feb15... edited to be correct "non-stagger"
        self.__moduleDiCounterPosition[self.__item[0]][tempDiCounterPosition] = tempDiCounterPosition
        self.__moduleDiCounterId[self.__item[0]][tempDiCounterPosition] = 'di-'+self.__item[n].strip()
        tempDiCounterPosition += 1
    tempDiCounterPosition = 0
    if(self.__item[0] == 'layer3'):
      #for n in range(3,11):
      for n in range(2,10): # BTuffs 2021Feb15... edited to be correct "non-stagger"
        self.__moduleDiCounterPosition[self.__item[0]][tempDiCounterPosition] = tempDiCounterPosition
        self.__moduleDiCounterId[self.__item[0]][tempDiCounterPosition] = 'di-'+self.__item[n].strip()
        tempDiCounterPosition += 1
    tempDiCounterPosition = 0
    if(self.__item[0] == 'layer4'):
      #for n in range(2,10):
      for n in range(2,10): # BTuffs 2021Feb15... edited to be correct "non-stagger"
        self.__moduleDiCounterPosition[self.__item[0]][tempDiCounterPosition] = tempDiCounterPosition
        self.__moduleDiCounterId[self.__item[0]][tempDiCounterPosition] = 'di-'+self.__item[n].strip()
        tempDiCounterPosition += 1
    ##
    ##  Read the Dicounter Id's from the SMB spreadsheets.
    cellIncrement = 1
    if(self.__item[0] == 'Layer-1-A'):
      #columnNumber = 14 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      for n in range(0,8):
        self.__moduleDiCounterPosition[self.__item[0]][n] = n
        self.__moduleDiCounterId[self.__item[0]][n] = 'di-'+self.__item[columnNumber].strip()
        columnNumber += cellIncrement
    if(self.__item[0] == 'Layer-2-A'):
      #columnNumber = 10 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      for n in range(0,8):
        self.__moduleDiCounterPosition[self.__item[0]][n] = n
        self.__moduleDiCounterId[self.__item[0]][n] = 'di-'+self.__item[columnNumber].strip()
        columnNumber += cellIncrement
    if(self.__item[0] == 'Layer-3-A'):
      #columnNumber = 6  ## start at offset to first cell
      columnNumber = 1  ## start at offset to first cell
      for n in range(0,8):
        self.__moduleDiCounterPosition[self.__item[0]][n] = n
        self.__moduleDiCounterId[self.__item[0]][n] = 'di-'+self.__item[columnNumber].strip()
        columnNumber += cellIncrement
    if(self.__item[0] == 'Layer-4-A'):
      columnNumber = 1
      for n in range(0,8):
        self.__moduleDiCounterPosition[self.__item[0]][n] = n
        self.__moduleDiCounterId[self.__item[0]][n] = 'di-'+self.__item[columnNumber].strip()
        columnNumber += cellIncrement
    if(self.__cmjDebug > 6):  ## Diagnostic Print statements
      print("__crvModules::storeDicounterPositionNonStaggered__ : self.__moduleDiCounterPosition \n")
      tempLayer = self.__item[0]
      print("__crvModules::storeDicounterPositionNonStaggered__ ::: DiCounterPosition based upon [layer][postion]\n")
      for tempDiCounterPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
        print(("__crvModules::storeDicounterPositionNonStaggered__ self.__moduleDiCounterPosition[layer][position] = self.__moduleDiCounterPosition[%s][%s] = %s \n") % (tempLayer,tempDiCounterPosition,self.__moduleDiCounterPosition[tempLayer][tempDiCounterPosition]))
      print("__crvModules::storeDicounterPositionNonStaggered__  ----------------- \n")
      ##
      print("__crvModules::storeDicounterPositionNonStaggered__ ::: DiCounterId's based upon [layer][postion]\n")
      for tempDiCounterId in list(self.__moduleDiCounterPosition[tempLayer].keys()):
        print(("__crvModules::storeDicounterPositionNonStaggered__ self.__moduleDiCounterId[layer][position] =  self.__moduleDiCounterId[%s][%s] = %s \n") % (tempLayer,tempDiCounterId,self.__moduleDiCounterId[tempLayer][tempDiCounterId]))
      print("__crvModules::storeDicounterPositionNonStaggered__ .... Exit \n")
    
## -----------------------------------------------------------------
##  Store the Sipm Mother Board...
##  This function is assumes the DiCounter_id, layer and positions have
##  already been stored.
##  Read from the Smb/Cmb spreadsheet
  def storeSmbNonStaggered(self, tempLine):
    self.__item = []
    self.__item = tempLine.rsplit(',')
    if(self.__cmjDebug > 0): print("__crvModules::storeSmbNonStaggered__ ... Enter \n")
    if(self.__cmjDebug > 8): print(("__crvModules::storeSmbNonStaggered__ self.__item[0] = Layer = %s \n") % (self.__item[0]))
    if(self.__cmjDebug > 9): print(("__crvModules::storeSmbNonStaggered__ .self.__moduleDiCounterId = %s \n") % (self.__moduleDiCounterId))
    ##
    ##  Read Sipm Mother Board Ids' from the SMB spreadsheets.
    self.__tempLayer = 0
    #cellIncrement = 4
    cellIncrement = 1
    if(self.__item[0] == 'SMB-L1-A'):
      #columnNumber = 14 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = 'Layer-1-A'
      for n in range(0,8):      ## loop over all diCounters in a given layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-1-A'][n]
        tempSmbId = self.__item[columnNumber].strip()
        if(tempSmbId.upper()=='REFLECTOR'):
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)  ## cmj2020Jul22
        elif (tempSmbId.upper()=='ABSORBER'):
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)  ## cmj2020Jul22
        else:
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        self.__dummyCounter +=1
        columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L2-A'):
      #columnNumber = 10 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = 'Layer-2-A'
      for n in range(0,8):      ## loop over all diCounters in a given layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-2-A'][n]
        tempSmbId = self.__item[columnNumber].strip()
        #self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        if(tempSmbId.upper()=='REFLECTOR'):
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)  ## cmj2020Jul22
        elif (tempSmbId.upper()=='ABSORBER'):
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)  ## cmj2020Jul22
        else:
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        self.__dummyCounter +=1
        columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L3-A'):
      #columnNumber = 6  ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = 'Layer-3-A'
      for n in range(0,8):      ## loop over all diCounters in a given layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-3-A'][n]
        tempSmbId = self.__item[columnNumber].strip()
        #self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        if(tempSmbId.upper()=='REFLECTOR'):
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)  ## cmj2020Jul22
        elif (tempSmbId.upper()=='ABSORBER'):
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)  ## cmj2020Jul22
        else:
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        self.__dummyCounter +=1
      columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L4-A'):
      #columnNumber = 2 ## start at offfset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = 'Layer-4-A'
      for n in range(0,8):      ## loop over all diCounters in a given layer...
        tempDiCounterId = self.__moduleDiCounterId['Layer-4-A'][n]
        tempSmbId = self.__item[columnNumber].strip()
        #self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        if(tempSmbId.upper()=='REFLECTOR'):
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)  ## cmj2020Jul22
        elif (tempSmbId.upper()=='ABSORBER'):
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)  ## cmj2020Jul22
        else:
          self.__moduleSmbIdSideA[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        self.__dummyCounter +=1
        columnNumber += cellIncrement
    ## --- Other end of the module... side B
    ##
    if(self.__item[0] == 'SMB-L1-B'):
      #columnNumber = 2 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempSmbId = self.__item[columnNumber].strip()
        #self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        if(tempSmbId.upper()=='REFLECTOR'):
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)  ## cmj2020Jul22
        elif (tempSmbId.upper()=='ABSORBER'):
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)  ## cmj2020Jul22
        else:
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        self.__dummyCounter +=1
        columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L2-B'):
      #columnNumber = 6 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempSmbId = self.__item[columnNumber].strip()
        #self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        if(tempSmbId.upper()=='REFLECTOR'):
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)  ## cmj2020Jul22
        elif (tempSmbId.upper()=='ABSORBER'):
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)  ## cmj2020Jul22
        else:
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        self.__dummyCounter +=1
        columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L3-B'):
      #columnNumber = 10  ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempSmbId = self.__item[columnNumber].strip()
        #self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        if(tempSmbId.upper()=='REFLECTOR'):
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)  ## cmj2020Jul22
        elif (tempSmbId.upper()=='ABSORBER'):
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)  ## cmj2020Jul22
        else:
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        self.__dummyCounter +=1
        columnNumber += cellIncrement
    if(self.__item[0] == 'SMB-L4-B'):
      #columnNumber = 14  ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempSmbId = self.__item[columnNumber].strip()
        #self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        if(tempSmbId.upper()=='REFLECTOR'):
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)  ## cmj2020Jul22
        elif (tempSmbId.upper()=='ABSORBER'):
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)  ## cmj2020Jul22
        else:
          self.__moduleSmbIdSideB[tempDiCounterId] = "CrvSmb-"+tempSmbId.upper()  ## cmj2020Jul2
        self.__dummyCounter +=1
        columnNumber += cellIncrement
      ##
    if(self.__cmjDebug > 3):  ## Detail Diagnostic print statements
      if(self.__item[0].find('-A') != -1):
        print("__crvModules::storeSmbNonStaggered__ ... self.__moduleSmbIdSideA \n")
        tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempSmbId = self.__moduleSmbIdSideA[tempDiCounter]
          print(("__crvModules::storeSmbNonStaggered__   tempLayer = %s || tempPosition = %s || tempDiCounter = %s || self.__moduleSmbIdSideA = %s \n") % (tempLayer,tempPosition,self.__moduleDiCounterId[tempLayer][tempPosition],self.__moduleSmbIdSideA[tempDiCounter]))
      if(self.__item[0].find('-B') != -1):
        print("__crvModules::storeSmbNonStaggered__ ... self.__moduleSmbIdSideB \n")
        tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempSmbId = self.__moduleSmbIdSideB[tempDiCounter]
          print(("__crvModules::storeSmbNonStaggered__   tempLayer = %s || tempPosition = %s || tempDiCounter = %s || self.__moduleSmbIdSideB = %s \n") % (tempLayer,tempPosition,self.__moduleDiCounterId[tempLayer][tempPosition],self.__moduleSmbIdSideB[tempDiCounter]))
    ##
    if(self.__cmjDebug > 0):  ## Diagnostic print statements
      if(self.__item[0].find('-A') != -1):
        print("__crvModules::storeSmbNonStaggered__ ... self.__moduleSmbIdSideA \n")
        tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempSmbId = self.__moduleSmbIdSideA[tempDiCounter]
          print(("__crvModules::storeSmbNonStaggered__  self.__moduleSmbIdSideA[tempDiCounter] = self.__moduleSmbIdSideA[%s] = %s\n") % (tempDiCounter,self.__moduleSmbIdSideA[tempDiCounter]))
      if(self.__item[0].find('-B') != -1):
        print("__crvModules::storeSmbNonStaggered__ ... self.__moduleSmbIdSideB \n")
        tempLayer = self.__moduleSmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempSmbId = self.__moduleSmbIdSideB[tempDiCounter]
          print(("__crvModules::storeSmbNonStaggered__  self.__moduleSmbIdSideB[tempDiCounter] = self.__moduleSmbIdSideB[%s] = %s\n") % (tempDiCounter,self.__moduleSmbIdSideB[tempDiCounter]))
      print("__crvModules::storeSmbNonStaggered__ ... Exit \n")
      
## -----------------------------------------------------------------
##  Store the Counter Mother Board...
##  This function is assumes the DiCounter_id, layer and positions have
##  already been stored.
##  Read from the Cmb/Smb spreadsheets
  def storeCmbNonStaggered(self, tempLine):
    self.__item = []
    self.__item = tempLine.rsplit(',')
    if(self.__cmjDebug > 0): print("__crvModules::storeCmbNonStaggered__ ... Enter \n")
    if(self.__cmjDebug > 9): print(("__crvModules::storeCmbNonStaggered__ ... self.__item[0] = %s\n") % (self.__item[0]))
    ##  Read Counter Mother Board Ids' from the SMB spreadsheets.
    #cellIncrement = 4
    cellIncrement = 1
    if(self.__item[0] == 'CmbId-L1-A'):
      #columnNumber = 14 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer...
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][n]
        tempCmbId = self.__item[columnNumber].strip()
        #self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        if(self.__moduleSmbIdSideA[tempDiCounterId].find('REFLECTOR') != -1):
          self.__moduleCmbIdSideA[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)
        elif(self.__moduleSmbIdSideA[tempDiCounterId].find('ABSORBER') != -1):
          self.__moduleCmbIdSideA[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)
        else:
          self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        self.__dummyCounter += 1
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L2-A'):
      #columnNumber = 10 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer...
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][n]
        tempCmbId = self.__item[columnNumber].strip()
        #self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        if(self.__moduleSmbIdSideA[tempDiCounterId].find('REFLECTOR') != -1):
          self.__moduleCmbIdSideA[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)
        elif(self.__moduleSmbIdSideA[tempDiCounterId].find('ABSORBER') != -1):
          self.__moduleCmbIdSideA[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)
        else:
          self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        self.__dummyCounter += 1
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L3-A'):
      #columnNumber = 6  ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer...
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][n]
        tempCmbId = self.__item[columnNumber].strip()
        #self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        if(self.__moduleSmbIdSideA[tempDiCounterId].find('REFLECTOR') != -1):
          self.__moduleCmbIdSideA[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)
        elif(self.__moduleSmbIdSideA[tempDiCounterId].find('ABSORBER') != -1):
          self.__moduleCmbIdSideA[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)
        else:
          self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        self.__dummyCounter += 1
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L4-A'):
      #columnNumber = 2 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer...
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][n]
        tempCmbId = self.__item[columnNumber].strip()
        #self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        if(self.__moduleSmbIdSideA[tempDiCounterId].find('REFLECTOR') != -1):
          self.__moduleCmbIdSideA[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)
        elif(self.__moduleSmbIdSideA[tempDiCounterId].find('ABSORBER') != -1):
          self.__moduleCmbIdSideA[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)
        else:
          self.__moduleCmbIdSideA[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        self.__dummyCounter += 1
        columnNumber += cellIncrement
    ## --- Other end of the module... side B
    ##
    if(self.__item[0] == 'CmbId-L1-B'):
      #columnNumber = 2 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempCmbId = self.__item[columnNumber].strip()
        #self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        if(self.__moduleSmbIdSideB[tempDiCounterId].find('REFLECTOR') != -1):
          self.__moduleCmbIdSideB[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)
        elif(self.__moduleSmbIdSideB[tempDiCounterId].find('ABSORBER') != -1):
          self.__moduleCmbIdSideB[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)
        else:
          self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        self.__dummyCounter += 1
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L2-B'):
      #columnNumber = 6 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempCmbId = self.__item[columnNumber].strip()
        #self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        if(self.__moduleSmbIdSideB[tempDiCounterId].find('REFLECTOR') != -1):
          self.__moduleCmbIdSideB[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)
        elif(self.__moduleSmbIdSideB[tempDiCounterId].find('ABSORBER') != -1):
          self.__moduleCmbIdSideB[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)
        else:
          self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        self.__dummyCounter += 1
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L3-B'):
      #columnNumber = 10  ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempCmbId = self.__item[columnNumber].strip()
        #self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        if(self.__moduleSmbIdSideB[tempDiCounterId].find('REFLECTOR') != -1):
          self.__moduleCmbIdSideB[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)
        elif(self.__moduleSmbIdSideB[tempDiCounterId].find('ABSORBER') != -1):
          self.__moduleCmbIdSideB[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)
        else:
          self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        self.__dummyCounter += 1
        columnNumber += cellIncrement
    if(self.__item[0] == 'CmbId-L4-B'):
      #columnNumber = 14
      columnNumber = 1 ## start at offset to first cell
      self.__tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
      for n in range(0,8):      ## loop over all diCounters in a given layer... other side... reflect dicounter position
        tempDiCounterId = self.__moduleDiCounterId[self.__tempLayer][7-n]
        tempCmbId = self.__item[columnNumber].strip()
        #self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        if(self.__moduleSmbIdSideB[tempDiCounterId].find('REFLECTOR') != -1):
          self.__moduleCmbIdSideB[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_REFLECTOR_'+str(self.__dummyCounter)
        elif(self.__moduleSmbIdSideB[tempDiCounterId].find('ABSORBER') != -1):
          self.__moduleCmbIdSideB[tempDiCounterId]="CrvCmb-"+self.__moduleId+'_ABSORBER_'+str(self.__dummyCounter)
        else:
          self.__moduleCmbIdSideB[tempDiCounterId] = "CrvCmb-"+tempCmbId.upper()  ## cmj 2020Jul02
        self.__dummyCounter += 1
        columnNumber += cellIncrement
    ##      Detailed Diagnostic print statements
    if(self.__cmjDebug > 3): 
      if(self.__item[0].find('-A') != -1):
        print("__crvModules::storeCmbNonStaggered__ ... self.__moduleCmbIdSideA \n")
        tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempCmbId = self.__moduleCmbIdSideA[tempDiCounter]
          print(("__crvModules::storeCmbNonStaggered__   tempLayer = %s || tempPosition = %s || tempDiCounter = %s || self.__moduleCmbIdSideA = %s \n") % (tempLayer,tempPosition,self.__moduleDiCounterId[tempLayer][tempPosition],self.__moduleCmbIdSideA[tempDiCounter]))
      if(self.__item[0].find('-B') != -1):
        print("__crvModules::storeCmbNonStaggered__ ... self.__moduleCmbIdSideB \n")
        tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempCmbId = self.__moduleCmbIdSideB[tempDiCounter]
          print(("__crvModules::storeCmbNonStaggered__   tempLayer = %s || tempPosition = %s || tempDiCounter = %s || self.__moduleCmbIdSideB = %s \n") % (tempLayer,tempPosition,self.__moduleDiCounterId[tempLayer][tempPosition],self.__moduleCmbIdSideB[tempDiCounter]))
##      Diagnostic Print statements
    if(self.__cmjDebug > 3): 
      if(self.__item[0].find('-A') != -1):
        print("__crvModules::storeCmbNonStaggered__ ... self.__moduleCmbIdSideA \n")
        tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempCmbId = self.__moduleCmbIdSideA[tempDiCounter]
          print(("__crvModules::storeCmbNonStaggered__   self.__moduleCmbIdSideA[diCounter] = self.__moduleCmbIdSideA[diCounter[%s] = %s \n") % (tempDiCounter,self.__moduleCmbIdSideA[tempDiCounter]))
      if(self.__item[0].find('-B') != -1):
        print("__crvModules::storeCmbNonStaggered__ ... self.__moduleCmbIdSideB \n")
        tempLayer = self.__moduleCmbToModuleLayer_dict[self.__item[0]]
        for tempPosition in list(self.__moduleDiCounterPosition[tempLayer].keys()):
          tempDiCounter = self.__moduleDiCounterId[tempLayer][tempPosition]
          tempCmbId = self.__moduleCmbIdSideB[tempDiCounter]
          print(("__crvModules::storeCmbNonStaggered__   self.__moduleCmbIdSideB[diCounter] = self.__moduleCmbIdSideB[diCounter[%s] = %s \n") % (tempDiCounter,self.__moduleCmbIdSideB[tempDiCounter]))
        print("__crvModules::storeCmbNonStaggered__ ... Exit \n")
##
## ----------------------------------------------------------------
##  Store the Sipm Id for Sipms on the end of dicounters... 
  def storeDicounterSipmIdNonStaggered(self, tempLine):
    self.__item = []
    self.__item = tempLine.rsplit(',')
    self.__crvSipmBatch = "CrvSipm-S14283(ES2)_"
    if(self.__cmjDebug > 0): print("__crvModules::storeDicounterSipmIdNonStaggered__ ... Enter xx \n")  
    if(self.__cmjDebug > 5): print(("__crvModules::storeDicounterSipmIdNonStaggered__ ...  self.__moduleDiCounterId = %s \n") % (self.__moduleDiCounterId))
    ## --
    ##  Read Sipm Ids' from the SMB spreadsheets.
    #diCounterCellIncrement = 4
    diCounterCellIncrement = 1
    cellIncrement = 1
    if(self.__item[0] == 'SipmId-L1-A'):
      #columnNumber = 14 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-1-A'][n]
        for m in range(0,4): ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L2-A'):
      #columnNumber = 10 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-2-A'][n]
        for m in range(0,4):  ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L3-A'):
      #columnNumber = 6  ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-3-A'][n]
        for m in range(0,4):
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L4-A'):
      #columnNumber = 2
      columnNumber = 1 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-4-A'][n]
        for m in range(0,4):  ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    ## --- Other end of the module... side B
    ##
    if(self.__item[0] == 'SipmId-L1-B'):
      #columnNumber = 2 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer... This layer viewed on other end... use origional layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-1-A'][7-n]
        for m in range(0,4):  ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m+4]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L2-B'):
      #columnNumber = 6 ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer... This layer viewed on other end... use origional layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-2-A'][7-n]
        for m in range(0,4):  ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m+4]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L3-B'):
      #columnNumber = 10  ## start at offset to first cell
      columnNumber = 1 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer... This layer viewed on other end... use origional layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-3-A'][7-n]
        for m in range(0,4):
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m+4]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    if(self.__item[0] == 'SipmId-L4-B'):
      #columnNumber = 14
      columnNumber = 1 ## start at offset to first cell
      for n in range(0,8):  ## Loop over all diCounter in this layer... This layer viewed on other end... use origional layer
        tempDiCounterId = self.__moduleDiCounterId['Layer-4-A'][7-n]
        for m in range(0,4):  ## loop over Sipms in a diCounter
          tempSipmPosition = self.__diCounterSipmLocation_dict[self.__diCounterSipms_list[m+4]]
          tempSipmId = self.__item[columnNumber].strip()
          self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition] = self.__crvSipmBatch+tempSipmId.upper()  ## 2020Jul02 make upper case
          columnNumber += cellIncrement
    ## ------ Diagnostic output
    if(self.__cmjDebug > 4): 
      print("__crvModules::storeDicounterSipmIdNonStaggered__ \n")
      print(("__crvModules::storeDicounterSipmIdNonStaggered__ self.__moduleSipmToModuleLayer_dict = %s\n") %(self.__moduleSipmToModuleLayer_dict))
      print(("__crvModules::storeDicounterSipmIdNonStaggered__ self.__item[0] = xx%sxx\n") % (self.__item[0]))
      mLayer = self.__moduleSipmToModuleLayer_dict[self.__item[0].strip()]
      nLayer = self.__moduleSipmToModuleLayerOneSide_dict[self.__item[0].strip()]
      print(("__crvModules::storeDicounterSipmIdNonStaggered__ Layer = %s \n") %(nLayer))
      for mDiCounter in range(0,8): ## loop over all di-counters in a layer
        print(("---------> mLayer = %s || mDiCounter = %s \n") % (mLayer,mDiCounter))
        tempDiCounterId = self.__moduleDiCounterId[nLayer][mDiCounter]  ## find the diCounter Id
        print(("---------> mLayer = %s || mDiCounter = %s || tempDiCounterId = %s \n") % (mLayer,mDiCounter,tempDiCounterId))
        for tempSipmPosition in sorted(self.__diCounterSipmLocation_dict.keys()):  ## Find the SipmId for a diCounter: use [diCounterId][SipmPosition]
          if(tempSipmPosition.find('A') != -1 and mLayer.find('-A') != -1): print(("__crvModules::storeDicounterSipmIdNonStaggered__ self.__moduleDiCounterSipmId[diCounterId][SipmPos] = self.__moduleDiCounterSipmId[%s][%s] = %s\n") % (tempDiCounterId,tempSipmPosition,self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition]))
          if(tempSipmPosition.find('B') != -1 and mLayer.find('-B') != -1): print(("__crvModules::storeDicounterSipmIdNonStaggered__ self.__moduleDiCounterSipmId[diCounterId][SipmPos] = self.__moduleDiCounterSipmId[%s][%s] = %s\n") % (tempDiCounterId,tempSipmPosition,self.__moduleDiCounterSipmId[tempDiCounterId][tempSipmPosition]))
      print("__crvModules::storeDicounterSipmIdNonStaggered__ ... Exit \n")
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
## ++++++++++++++++++++++++ End the CMB/SMB/SIPM Decode Files +++++++++++++++++++++++++++++++++++
## ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++




























## -----------------------------------------------------------------
##      Read in diCounter measured test results: option 3: "measure"
  def storeModuleMeasure(self,tempCounter):
    self.__item = []
    self.__item = tempNewLine.rsplit(',')
    self.__moduleId[self.__item[0]] = self.__item[0]
    self.__moduleTestDate[self.__item[0]] = self.timeStamper(self.__item[1])
    self.__moduleTestLightSource[self.__item[0]] = self.__item[2] 
    self.__moduleTestLightYieldAverage[self.__item[0]] = self.__item[3]
    self.__moduleTestLightYieldStDev[self.__item[0]] = self.__item[4]
    self.__moduleTestComments[self.__item[0]] = self.__item[5]

##
##
## -----------------------------------------------------------------
## -----------------------------------------------------------------
##      Utility methods...
##
##
## -----------------------------------------------------------------
##  This method translates the excel spreadiCounterd sheet date into the
##  format expected by the timestamp used in the database
##
  def dateStamper(self,tempInput):
    self.__tempDate = tempInput
    self.__tempMmDdYy = {}
    self.__tempMmDdYy = self.__tempDate.rsplit('/',3)
    self.__tempMonth = self.__tempMmDdYy[0]
    if (int(self.__tempMonth) < 10): self.__tempMonth = '0'+self.__tempMonth 
    self.__tempDay   = self.__tempMmDdYy[1]
    if (int(self.__tempDay) < 10 ): self.__tempDay = '0'+self.__tempDay
    self.__tempYear  = self.__tempMmDdYy[2]
    if (int(self.__tempYear) < 2000 ): self.__tempYear = '20'+self.__tempYear
    self.__tempDateStamp = self.__tempMonth+'/'+self.__tempDay+'/'+self.__tempYear
    if(self.__cmjDebug > 11):
      print(("XXXX__crvModules__:dateStamper...... self.__tempDate      = <%s>") % (self.__tempDate))
      print(("XXXX__crvModules__:dateStamper...... self.__tempMmDdYy    = <%s>") % (self.__tempMmDdYy))
      print(("XXXX__crvModules__:dateStamper...... self.__tempMonth     = <%s>") % (self.__tempMonth))
      print(("XXXX__crvModules__:dateStamper....crvModules.. self.__tempDay       = <%s>") % (self.__tempDay))
      print(("XXXX__crvModules__:dateStamper...... self.__tempYear      = <%s>") % (self.__tempYear))
    if(self.__cmjDebug > 10):
      print(("XXXX__crvModules__:dateStamper...... self.__tempDateStamp = <%s>") % (self.__tempDateStamp))
    return self.__tempDateStamp


## -----------------------------------------------------------------
##  This method translates the excel spread sheet date and time into the
##  format expected by the timestamp used in the database
##
  def timeStamper(self,tempInput):
    #  The following code is to compensate between the different time formats between excel & database
    #  Begin excel/timestamp format translation.
    #  Allowable formats for production_date are  YYYY-MM-DD or MM/DD/YYYY with year in range 2000-2049
    self.__tempDate = tempInput
    self.__tempMmDdYy = {}
    self.__tempMmDdYy = self.__tempDate.rsplit('/',3)
    self.__tempMonth = self.__tempMmDdYy[0] 
    if (int(self.__tempMonth) < 10): self.__tempMonth = '0' + str(self.__tempMonth)
    self.__tempDay = self.__tempMmDdYy[1]
    if (int(self.__tempDay) < 10) : self.__tempDay = '0'+str(self.__tempDay)
    self.__tempCombined = {}
    self.__tempCombined = self.__tempMmDdYy[2]
    self.__tempYyHM = {}
    self.__tempYyHM = self.__tempCombined.rsplit(' ',2)
    self.__tempYear = self.__tempYyHM[0]
    self.__tempTime = {}
    self.__tempTime = self.__tempYyHM[1].rsplit(':',2)
    self.__tempHour = self.__tempTime[0]

    self.__tempMin  = self.__tempTime[1]
    if(int(self.__tempHour) < 10): self.__tempHour = '0'+ self.__tempHour
    if(int(self.__tempMin) < 10): self.__tempMin = '0' + self.__tempMin
    self.__tempTimeStamp = '20'+self.__tempYear+'-'+self.__tempMonth+'-self.__localDiCounterIndex'+self.__tempDay+' '+self.__tempHour+':'+self.__tempMin
    if(self.__cmjDebug > 11):
      print(("XXXX__crvModules__:timeStamper...... self.__tempDate     = <%s>") % (self.__tempDate))
      print(("XXXX__crvModules__:timeStamper...... self.__tempMmDdYy   = <%s>") % (self.__tempMmDdYy))
      print(("XXXX__crvModules__:timeStamper...... self.__tempMonth    = <%s>") % (self.__tempMonth))
      print(("XXXX__crvModules__:timeStamper...... self.__tempDay      = <%s>") % (self.__tempDay))
      print(("XXXX__crvModules__:timeStamper...... self.__tempCombined = <%s>") % (self.__tempCombined))
      print(("XXXX__crvModules__:timeStamper...... self.__tempYear     = <%s>") % (self.__tempYear))
      print(("XXXX__crvModules__:timeStamper...... self.__tempHour  = <%s>") % (self.__tempHour))
      print(("XXXX__crvModules__:timeStamper...... self.__tempMin   = <%s>") % (self.__tempMin))
    if(self.__cmjDebug > 10):
      print(("XXXX__crvModules__:timeStamper....... self.__tempTimeStamp   = <%s>") % (self.__tempTimeStamp))
    #  End excel/timestamp format translation.
    return self.__tempTimeStamp

## -----------------------------------------------------------------
##

##############################################################################################
##############################################################################################
##  Entry point to program if this file is executed...
if __name__ == '__main__':
  parser = optparse.OptionParser("usage: %prog [options] file1.txt ")
#      Build general help string
  modeString = []
  modeString.append("To run in default mode (add module to database):")
  modeString.append("> python Modules.py -i ModuleSpreadsheet.cvs")
  modeString.append("To run to add module test results to database:")
  modeString.append("> python Modules.py -i ModuleTestSpreadsheet.cvs --mode ''measure''")
  modeString.append("The user may use a relative or absolute path to the spreadsheet ")
#      Input comma separated file name:
  parser.add_option('-i',dest='inputCvsFile',type='string',default="",help=modeString[0]+"\t\t\t"+modeString[1]+"\t\t\t"+modeString[2]+"\t\t\t"+modeString[3]+"\t\t\t\t\t\t\t"+modeString[4])
  modeString1 =[]
  modeString1.append("Input Mode: This script is run in several modes: \t\t\t")
  modeString1.append("initial: The default mode enters the initial module information. \t\t\t\t\t")
  modeString1.append("measure: This mode enters module test results into the database... Multiple test may be entered. ")
  parser.add_option('--mode',dest='inputMode',type='string',default="iniself.__localDiCounterIndextial",help=modeString1[0]+modeString1[1]+modeString1[2])
#      Debug level
  parser.add_option('-d',dest='debugMode',type='int',default=0,help='set debug: 0 (off - default), 1 = on')
  parser.add_option('-t',dest='testMode',type='int',default=0,help='set to test mode (do not send to database): 1')
  options, args = parser.parse_args()
  inputCounterFile = options.inputCvsFile
  if(inputCounterFile == ''):
    print("Supply input spreadsheet comma-separated-file")
    for outString in modeString:
      print(("%s") % outString)
      exit()
  inputModeValue = options.inputMode
  print(("\nRunning %s \n") % (ProgramName))
  print(("%s \n") % (Version))
  print("inputCounterFile = %s " % inputCounterFile)
  myCrvModules = crvModules()
  if(options.debugMode == 0):
    myCrvModules.turnOffDebug()
  else:
    myCrvModules.turnOnDebug(options.debugMode)
  print(("__main__ options.testMode = %s \n") % (options.testMode))
  if(options.testMode == 0):
    print("__main__ send to database! \n")
    myCrvModules.sendToDevelopmentDatabase()  ## turns on send to database
    #myCrvModules.sendToProductionDatabase()  ## turns on send to database
  else:
    myCrvModules.turnOffSendToDatabase()
  ## --------------------------------------------
  if(options.debugMode == 1): print('__name__ inputModeValue = %s \n' % inputModeValue)
  myCrvModules.openFile(inputCounterFile)
  myCrvModules.openLogFile()
  myCrvModules.readFile(inputModeValue)
  myCrvModules.sendToDatabase(inputModeValue) 
#
  print(("Finished running %s \n") % (ProgramName))
#

