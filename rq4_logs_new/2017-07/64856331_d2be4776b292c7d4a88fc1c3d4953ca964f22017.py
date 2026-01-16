#!/usr/bin/env python

from mpi4py import MPI
import matplotlib
# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')

import os, os.path, sys, json, argparse, shutil, re, subprocess

def compareCollections(collection1, collection2):
	# Check to see if two collections have the same filenames in them
	if len(collection1.objectList) != len(collection2.objectList):
		print "The two lists are different lengths. Have you added/removed files since creating the status file?"
	for c in collection1.objectList:
		filename = c['filename']
		found = False
		for c2 in collection2.objectList:
			if c2['filename'] == filename:
				found = True
		if not found:
			print "warning:", filename, "is not found in both lists"
			return False
	return True

class FITScollection:
	def __init__(self):
		self.objectList = []
			
	def additem(self, filename):
		fitsObject = {}
		
		fitsObject['baseFilename'] = os.path.basename(filename)
		fitsObject['filename'] = filename
		fitsObject['processed'] = False
		self.objectList.append(fitsObject)
	
	def getFilenames(self):
		return [f['filename'] for f in self.objectList]
		
	def sort(self):
		self.objectList = sorted(self.objectList, key=lambda object: object['filename'], reverse = False)
		
	def writeToCSV(self, filename):
		outfile = open(filename, 'wt')
		for o in self.objectList:
			outfile.write("%s, %s\n"%(o['filename'], o['processed']))
		outfile.close()

	def loadFromCSV(self, filename):
		infile = open(filename, 'rt')
		
		for l in infile:
			fitsObject = {}
			l = l.strip()
			items = l.split(',')
			filename = items[0].strip()
			if items[1].strip() == 'True':
				status = True
			else:
				status = False
			fitsObject['filename'] = filename
			fitsObject['processed'] = status
			self.objectList.append(fitsObject)
		infile.close()
		
	def updateStatus(self, newStatus):
		for o in self.objectList:
			filename = o['filename']
			found = False
			changed = False
			oldStatus = o['processed']
			for c in newStatus.objectList:
				if c['filename'] == filename:
					found = True
					if oldStatus != c['processed']:
						changed = True
						o['processed'] = c['processed']
						print "changed status of", filename, "to", o['processed']

				
	def __str__(self):
		return "%d objects in the list."%len(self.objectList)
		
	

if __name__ == '__main__':
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank()

    if rank == 0:
	parser = argparse.ArgumentParser(description='A simple Python tool to run hagrid in a batch mode over a folder full of files or a list of files.')
	parser.add_argument('-p', '--path', type=str, help='The folder in which the IPHAS images are contained.')
	parser.add_argument('-l', '--list', type=str, help='Pass in a list of filenames as a textfile (one filename per line).')
	parser.add_argument('-w', '--workingpath', type=str, help='A root folder for the temporary and output files.')
	parser.add_argument('-s', '--script', type=str, help='The script to use. By default is will look in the local directory for a file called ''script.template''.')
	arg = parser.parse_args()
	print arg
    else:
        arg = None
    arg=comm.bcast(arg,root=0)

    if arg.list is None:
	operateFromList = False
    else: operateFromList = True
       
    if arg.path is None:
	dataPath="."
    else: 
	dataPath= arg.path
    if arg.workingpath is None:
	workingPath="."
    else: 
	workingPath= arg.workingpath
	
    if arg.script is None:
	scriptFile = "script.template"
    else:
	scriptFile = arg.script

    if rank == 0:
	allObjects = FITScollection()	
	if not operateFromList:
		# Get a list of files in the folder
		# First, check if the source data is there
		if not os.path.exists(dataPath):
			print "The folder for the source data %s could not be found. Exiting."%dataPath
			sys.exit()
	
		searchString = "r.*.fits.fz"
		search_re = re.compile(searchString)
		 
		(_, _, filenames) = os.walk(dataPath).next()
		for file in filenames:
			m = search_re.match(file)
			if (m): 
				allObjects.additem(file)
	else:
		print "Getting filenames from the list: %s"%arg.list
		fileList = open(arg.list, 'rt')
		for line in fileList:
			allObjects.additem(line.strip())
		
	print allObjects
	allObjects.sort()
	
	# Second, check to see if the working directory already exists
	if not os.path.exists(workingPath):
		print("Creating folder %s"%workingPath)
		os.makedirs(workingPath)
		
	# Third check to see if a status.csv file exists, if so load it.
	if os.path.exists(workingPath + "/status.csv"):
		print "found an existing 'status.csv' file. Loading it."
		existingStatus = FITScollection()
		existingStatus.loadFromCSV(workingPath + "/status.csv")
		compareCollections(allObjects, existingStatus)
		allObjects.updateStatus(existingStatus)
	
	allObjects.writeToCSV(workingPath + "/status.csv")	
	
	# loop over all files.
        # check if the file needs to be processed. Distribute the processing
        # by using a master (0) / slave algorithm.
        # Make sure IO to status.csv only happens at the master.
        worklist={}
	for index, o in enumerate(allObjects.objectList):
	    status = o['processed']
	    filename = o['filename']
	    if status == True: 
		print "skipping", filename
		continue
	
            # check who wants work
            wnode=comm.recv(source=MPI.ANY_SOURCE,tag=101)
            # check previous work by that node
            if wnode in worklist:
	        # Before marking status as processed, check to see if the output files are there
	        baseFilename = worklist[wnode]['baseFilename']
		rootName = baseFilename.split(".")[0]
		outputFileCheck = workingPath + "/" + rootName + ".sources.fits"
		print "Checking for output at:", outputFileCheck
		if os.path.exists(outputFileCheck):
			worklist[wnode]['processed'] = True
		else: 
		    print "Hagrid failed. Please check for errors."
		    with open(workingPath + "/failed.log", "a") as errorLog:
			errorLog.write(worklist[wnode]['filename'] + "\n")
			sys.stdin.read(1)
			
		allObjects.writeToCSV(workingPath + "/status.csv")	
	    print "Processing", filename, "on node", wnode
            comm.send(filename,dest=wnode,tag=102)
            worklist[wnode]=o

        # now we need to send finish messages to all the nodes
        for i in range(size-1):
            wnode=comm.recv(source=MPI.ANY_SOURCE,tag=101)
            comm.send("",dest=wnode,tag=102)
            # check previous work by that node
            if wnode in worklist:
	        # Before marking status as processed, check to see if the output files are there
	        baseFilename = worklist[wnode]['baseFilename']
		rootName = baseFilename.split(".")[0]
		outputFileCheck = workingPath + "/" + rootName + ".sources.fits"
		print "Checking for output at:", outputFileCheck
		if os.path.exists(outputFileCheck):
			worklist[wnode]['processed'] = True
		else: 
		    print "Hagrid failed. Please check for errors."
		    with open(workingPath + "/failed.log", "a") as errorLog:
			errorLog.write(worklist[wnode]['filename'] + "\n")
			sys.stdin.read(1)
			
		allObjects.writeToCSV(workingPath + "/status.csv")	
	
    else:
        # -------------
        # slave section
        # -------------
	# Load the setup template
	setupFile = open(scriptFile, 'rt')
	templateString = setupFile.read()
	setupFile.close()

        while True:
	    # Ask for work and check if all is finished
            comm.send(rank,dest=0,tag=101)
            filename=comm.recv(source=0,tag=102)
            if filename=="": break
	    # Fill the template
            fn=os.path.normpath(os.path.join(dataPath,filename))
	    scriptString = templateString.format(filename = fn, workingpath = workingPath, datapath = dataPath, root = '{root}')
	    # Write it to a script file
            scriptName=workingPath + "/script.hagrid."+str(rank)
	    setupFile = open(scriptName, 'wt')
	    setupFile.write(scriptString)
	    setupFile.close()

	    #hagridCommand = ["hagrid"]
	    hagridCommand = ["/home/greimel/Software/bin/python","hagrid.py"]
	    hagridCommand.append(scriptName)
			
            outfn=os.path.join(workingPath,os.path.split(filename)[1].replace("fits.fz","stdout"))
            outfh=open(outfn,'wt')
	    subprocess.call(hagridCommand,stdout=outfh)
            outfh.close()
	