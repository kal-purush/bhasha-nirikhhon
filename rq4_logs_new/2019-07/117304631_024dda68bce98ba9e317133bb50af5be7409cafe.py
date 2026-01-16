#!/usr/bin/python
import os
import re
import sys
import yaml
import timeit
import datetime
import uproot 
# use of uproot package. This improves the running time by more than factor of 10
#from colorpicker import * 
import colorpicker

from tqdm import tqdm


start = timeit.default_timer() # start timer

import argparse

parser = argparse.ArgumentParser(description='User inputs')

parser.add_argument('-d','--hadd',
		    action='store_false', 
		    default=False,
		    help='Tell if you need to perform only hadd on the similar root files or not (default: False, it means it will perform hadd as well as it will generate the text file.)'
		   ) # ifhaddOnly
parser.add_argument('-p','--path',
		    action='store', 
		    default='/store/user/rasharma/SecondStep/Run_2017/Frameworkupdate/WWTree_2019_05_01_14h50/', 
		    required=True,
		    help='Path of store are where it will find the input root file (default = /store/user/rasharma/SecondStep/Run_2017/Frameworkupdate/WWTree_2019_05_01_14h50/).'
		   ) # StoreArea
parser.add_argument('-s','--save',
		    action='store', 
		    default='/uscms_data/d3/rasharma/aQGC_analysis/PlottingMacros/CMSSW_9_0_1/src/PlottingCodes2017/ControlPlots/SampleFiles/',
		    help='Add path of plotting code direcotory. After making the text file it will copy the same to this directory (default = /uscms_data/d3/rasharma/aQGC_analysis/PlottingMacros/CMSSW_9_0_1/src/PlottingCodes2017/ControlPlots/SampleFiles/).'
		   )  # PlottingDirectoryPath
parser.add_argument('-a','--arrange',
		    action='store', 
		    default=['# name', 'data', 'Data', 'WV_EWK', 'aQGC', 'Diboson', 'VV', 'W\+jets', 'Z\+jets', 'top', 'QCD'], 
		    help="Arrange the each line of the generated text file in the given order (default = ['# name', 'data', 'Data', 'WV_EWK', 'aQGC', 'Diboson', 'VV', 'W\+jets', 'Z\+jets', 'top', 'QCD'])"
		   ) # searchString
parser.add_argument('-o','--outdir',
		    action='store', 
		    default='HaddedFiles',
		    help='Name of output directory where it will place the hadd-ed root files (default = HaddedFiles)'
		   ) # StoreAreaHadd = StoreArea+'HaddedFiles_Test/'
parser.add_argument('-e','--eosstring',
		    action='store', 
		    default='/eos/uscms',
		    help='Add the store area initials using which one can access it locally (default = /eos/uscms).'
		   ) #source = "/eos/uscms"+StoreArea

args = parser.parse_args()

#print args

def CheckDirectory(path, directoryName):
    if os.path.isdir(args.eosstring+path+directoryName):
      print "\t","#"*51
      print(colorpicker.CRED+"\tDirectory "+path+directoryName+' found. Delete it...'+colorpicker.CEND)
      os.system('eos root://cmseos.fnal.gov/ rm -r '+path+directoryName)
      print "\t# list all file in main directory: Just to check if directory HaddedFiles_Test does not exists"
      os.system('ls '+args.eosstring+path+directoryName)
      print "\tCreate the directory"
      os.system('eos root://cmseos.fnal.gov/ mkdir '+path+directoryName)
      print "\t","#"*51
    else:
      print "Create the directory"
      os.system('eos root://cmseos.fnal.gov/ mkdir '+path+directoryName)

def GetFileNameWithPathInArray(path, fileExtension):
    Arrayfilepath = []
    for root, dirs, filenames in os.walk(args.eosstring+path):
      for f in filenames:
        filepath = f
	if filepath.endswith(fileExtension):
	  Arrayfilepath.append(filepath)
    return Arrayfilepath

def LoadYamlFile(filename):
    file = open(filename,"r")
    ymload = yaml.load(file)
    file.close()
    return ymload

def GetFileNeventsNnegativeEvents(YamlFileContent, Array_FileNameWithFullPath, path):
    ifhaddOnly = 0
    List = []
    ListnEvents = []
    ListnNegEvents = []
    for sample in YamlFileContent:
      temp = []
      temp.append(sample+".root")
      print(colorpicker.CBOLD+"====================> "+sample+colorpicker.CEND)
      nEvents=0
      nNegEvents=0
      for i,files in enumerate(Array_FileNameWithFullPath):
        if files.find(sample) != -1:
	  print "DEBUG: 1: ",args.eosstring+path+"/"+files
	  if os.path.isfile(args.eosstring+path+"/"+files):
	    if (uproot.open(args.eosstring+path+"/"+files).keys()) == []:
	      print (colorpicker.CGREEN+"\nskip file: "+files+"\n"+colorpicker.CEND)
	    else:
	      otree = uproot.open(args.eosstring+path+"/"+files)["otree"]
	      InputArrays = otree.arrays(["nEvents","nNegEvents"])
	      if len(InputArrays["nEvents"]):
	        if files.find('Single') != -1:
		  nEvents=1
		  nNegEvents=0
		else:
		  if ifhaddOnly != 1:
		    nEvents += InputArrays["nEvents"][0]
		    nNegEvents+= InputArrays["nNegEvents"][0] 
		  else:
		    print "skip..."
		temp.append(files)
		print files,nEvents,nNegEvents
	      else:
	        print(colorpicker.CRED+"\nFile Not Found: "+files+"\n"+colorpicker.CEND)
      if len(temp)>1:
  	List.append(temp)
  	ListnEvents.append(nEvents)
  	ListnNegEvents.append(nNegEvents)
    return List, ListnEvents, ListnNegEvents

def haddFilesInListofLists(ListOfListsHavingRootFiles):
    sampleInfo = []
    for ArrayOfRootFiles in ListOfListsHavingRootFiles:
      temp="hadd -f "
      for RootFiles in range(0,len(ArrayOfRootFiles)):
        if RootFiles == 0:
	  #temp+=args.eosstring+path+'HaddedFiles_Test/'+samples[i]+' '
	  temp+=samples[i]+' '
	else:
	  temp+=args.eosstring+path+"/"+samples[i]+" "
    print temp
    os.system(temp)
    sampleInfo.append(samples[0])
    print ""

def CreatePlottingTextFile():
    print "=============  MAKE SUMMARY    ================"  
    print(sampleInfo)
    
    OutPutFile = "DibosonBoostedElMuSamples13TeV_"+args.eosstring+path.split("/")[-4]+"_"+args.eosstring+path.split("/")[-3]+"_"+args.eosstring+path.split("/")[-2]+".txt";
    outScript = open(OutPutFile,"w");
    outScript.write("# name           file_location  xspb/lumipb  otherscale nMCevents       nMCNegEvents    colorcode       stackit\n")
    for i,files in enumerate(sampleInfo):
      for sample in ymload:
        if files.find(sample) != -1:
	  print ymload[sample]["name"],"\t",files,"\t",ymload[sample]["CrossSection"],"\t1\t",int(ListnEvents[i]),"\t",int(ListnNegEvents[i]),"\t",ymload[sample]["ColorCode"],"\t",ymload[sample]["StackIt"]
	  outScript.write(ymload[sample]["name"]+"\t"+StoreAreaHadd+files+"\t"+str(ymload[sample]["CrossSection"])+"\t1\t"+str(ListnEvents[i])+"\t"+str(ListnNegEvents[i])+"\t"+str(ymload[sample]["ColorCode"])+"\t"+str(ymload[sample]["StackIt"])+"\n")
    outScript.close()

def SortPlottingTextFile():
  ################################################
  #
  #	Sort the file manually
  #
  ################################################
  SortedOutPutFile = "Sorted_"+OutPutFile
  outScript2 = open(SortedOutPutFile,'w')
  
  for strings in searchString:
    infile = open(OutPutFile,"r")
    for lines in infile:
      if re.match(str(strings),lines): 
        #print lines,
	outScript2.write(lines)
  outScript2.close()
  
################################################


def main():
  print("Step: 1: Make list of all root files in eos.")
  CheckDirectory(args.path, args.outdir)
  print("Step: 2: Get list of file names")
  Array_FileNameWithFullPath = GetFileNameWithPathInArray(args.path, ".root")
  print Array_FileNameWithFullPath
  print "\n\n"
  YamlFileContent = LoadYamlFile("DataMCInfo.yml")
  print "\n\nYaml File content:\n"
  print YamlFileContent
  print "\n\n"
  fileName, nEvents, nNegEvents = GetFileNeventsNnegativeEvents(YamlFileContent, Array_FileNameWithFullPath, args.path)
  print "fileName , nEvents , nNegEvents = ",fileName, nEvents, nNegEvents
  print("Step: 2: Read each root file and saves its number of events and number of negative events in each file.")
  print("Step: 3: Club the root files based on the keys given in the yaml file")
  print("Step: 4: Do the hadd for each clubbed list.")
  print("Step: 5: Create the text file that contains all info for the plotting.")
  
  # copy the created text file to the respective directory
  #os.system('xrdcp -f '+OutPutFile+'  root://cmseos.fnal.gov/'+ StoreAreaHadd)
  #os.system('xrdcp -f '+SortedOutPutFile+'  root://cmseos.fnal.gov/'+ StoreAreaHadd)
  #os.system('cp '+OutPutFile+' '+ SortedOutPutFile + ' ' +PlottingDirectoryPath)
  # Stop timer
  #stop = timeit.default_timer()
  # Print total time to run in minutes
  #print 'Run Time: ',round((stop - start)/60.0,2),'min'  

if __name__ == "__main__":
  main()
