#!/usr/bin/python

'''
A small personal project to correct orthography in LaTeX and html files
Author: Robin Milosz

to run:
python words_correction_program.py [the path of your target file "/u/username/Documents/example.tex"]
'''

#imports
import time
import sys

#start counting the time
nm = 0
ts = time.time()

#this method tests if a word is "normal" and can be check in a dictionary
# it prevents a lot of LaTeX formatting words to be checked
def okKey(key):
    if key.find("$") != -1:
        return False
    elif key.find("(") != -1:
        return False
    elif key.find(")") != -1:
        return False
    elif key.find("[") != -1:
        return False
    elif key.find("]") != -1:
        return False
    elif key.find("{") != -1:
        return False
    elif key.find("}") != -1:
        return False
    elif key.find("<") != -1:
        return False
    elif key.find(">") != -1:
        return False
    elif key.find("%") != -1:
        return False
    elif key.find("\\") != -1:
        return False
    elif key.find("/") != -1:
        return False
    elif key.find("*") != -1:
        return False
    elif key.find("+") != -1:
        return False
    elif key.find("^") != -1:
        return False
    elif key.find("=") != -1:
        return False
    elif key.find("&") != -1:
        return False
    elif key.find("_") != -1:
        return False
    elif key.find("|") != -1:
        return False
    elif key.find("#") != -1:
        return False
    elif key.find(".") != -1: #websites
        return False
    elif key == "-":
        return False
    elif key.isdigit():
        return False
    else:
        return True

#welcome print
print " "
print " *** Word Correction Program ***"
print "  (originaly intended for french/english LaTeX or html files)"
print " "

#create empty dictionaries
frenchDictionary = {}
englishDictionary = {}
extraDictionary = {}

#load the french dictionary
dictionaryFileName = "french_words.txt"
filePath = "data/%s" % dictionaryFileName
count = 1
with open(filePath,"r") as dataFile:
    for line in dataFile:
        #array.append(line)
        #print line
        word = line.rstrip().strip()
        frenchDictionary[word] = count
        count += 1

dataFile.close()

#load the english dictionary
dictionaryFileName = "english_words.txt"
filePath = "data/%s" % dictionaryFileName
count = 1
with open(filePath,"r") as dataFile:
    for line in dataFile:
        #array.append(line)
        #print line
        word = line.rstrip().strip().lower()
        englishDictionary[word] = count
        count += 1

dataFile.close()

#load the extra words dictionary
dictionaryFileName = "extra_words.txt"
filePath = "data/%s" % dictionaryFileName
count = 1
with open(filePath,"r") as dataFile:
    for line in dataFile:
        #array.append(line)
        #print line
        word = line.rstrip().strip()
        extraDictionary[word] = count
        count += 1

dataFile.close()

#print the sizes of the dictionaries
print "frenchDictionary size:", len(frenchDictionary)
print "englishDictionary size:", len(englishDictionary)
print "extraDictionary size:", len(extraDictionary)
print " "

#specify the target file
if (len(sys.argv) > 1):
    filePath = sys.argv[1]
else:
    filePath = "my_text.txt"


#open the target file
lineNumber = 0
numberOfPotentialMistakes = 0
with open(filePath,"r") as dataFile:
    for line in dataFile: #read all the lines
        lineNumber += 1
        #print lineNumber," lines done         \r",
	#linePure = line.rstrip().strip().replace(';',' ').replace(',',' ').replace('.',' ').replace('\'',' ')
	linePure = line.rstrip().strip().replace(';',' ').replace(',',' ').replace('\'',' ')
	linePure2 = linePure.rstrip().strip().replace('\"',' ').replace('?',' ').replace(':',' ').replace('-',' ').replace('!',' ').replace('`',' ')
	#linePure3 = linePure.rstrip().strip().replace('(',' ').replace(')',' ').replace('{',' ').replace('}',' ')
        for word in linePure2.split(" "): #split the line into words
            pureWord = word.rstrip().strip() #take the blank spaces before and after
            if pureWord.find(".") != -1: #if there is a dot at the end of the word, take it off
                if pureWord[len(pureWord)-1] == '.':#so that website stay but no ending words
                    pureWord = pureWord[0:len(pureWord)-1]

            #key = pureWord.decode('utf-8').lower()
            key = pureWord.lower() #put it in lowercase
            if len(key) > 0:
                if okKey(key):
                    if key in frenchDictionary: #if it's in the french dictionary
                        #print "ok"
                        pass
                    elif key in englishDictionary: #if it's in the english dictionary
                        #print "ok"
                        pass
                    elif key in extraDictionary: #if it's in the extra words dictionary
                        #print "ok"
                        pass
                    else: #else it my be an error
                        numberOfPotentialMistakes += 1
                        print "%s (%s)" % (key,lineNumber) #print the potential mistake
                        #print "%s" % (key)
                        #print "%s" % (pureWord)
#close the file
dataFile.close()

#print the number of potential mistakes found
print ""
print "numberOfPotentialMistakes: %s" %numberOfPotentialMistakes
print ""

#print the execution time
st = time.time()-ts
print "total time:", st, "sec"
