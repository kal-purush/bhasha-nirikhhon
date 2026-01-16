import os
import operator

def listdir_fullpath(d):
    return [os.path.join(d, f) for f in os.listdir(d)]

def getWordFiles(rootDir):
    dirList = [rootDir]
    fileList = []
    while(len(dirList)!=0):
        for i in listdir_fullpath(dirList.pop()):
            if(os.path.isdir(i)):
                dirList.append(i)
            else:
                fileList.append(i)
    return fileList

def countWords(fileList):
    words = {}
    for i in fileList:
        wordFile = open(i, "r+")
        for word in wordFile.read().split():
            if word not in words:
                words[word] = 1
            else:
                words[word] += 1
    return words

def sortWords(wordList):
    return sorted(wordList.items(), key=operator.itemgetter(1), reverse=True)

def displayTop10(wordList):
    displayCount = 10
    if (len(wordList)<10):
        displayCount = len(wordList)

    for i in range(displayCount):
        print( str((i+1))+ ". "+ wordList[i][0] + "("+str(wordList[i][1])+" times)")

pathname = ""
while (os.path.isdir(pathname)==False):
    pathname = input('Enter a valid directory path: ')

fileList = getWordFiles(pathname)
words = countWords(fileList)
words = sortWords(words)
displayTop10(words)
