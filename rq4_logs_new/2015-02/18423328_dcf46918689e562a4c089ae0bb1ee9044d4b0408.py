#!/usr/bin/env python
#
# Constants
#
__VERSION__ = 0.1
__author__ = 'morrj140'

import os

# Common Filenames
homeDir = os.getcwd() + os.sep

documentsFile  = homeDir + "documents.p"
projectsFile   = homeDir + "projectsDict.p"
peopleFile     = homeDir + "peopleDict.p"
wordsFile      = homeDir + "wordsDict.p"
topicsFile     = homeDir + "topicsDict.p"
sentencesFile  = homeDir + "sentencesDict.p"
similarityFile = homeDir + "similarityDict.p"

imageFile      = homeDir + "Topics_Cloud.bmp"

gmlFile        = homeDir + "Concepts.gml"

logFile        = homeDir + "nl_phase_log.txt"