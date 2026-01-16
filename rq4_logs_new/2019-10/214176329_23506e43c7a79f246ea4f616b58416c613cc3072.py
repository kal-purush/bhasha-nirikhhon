def convertToCapital(listOfLines):
    listOfSQLKeywords = ["select","from","where","inner","natural","join","outer","right"
                        ,"left","full","having","as","create","view","is","null","on","using"
                        ,"count","not","like","and","or","order","by","group","desc","union","with"
                        ]

    wordCount = len(listOfSQLKeywords)
    for i in range(wordCount):
        listOfSQLKeywords.append("(" + listOfSQLKeywords[i])

    newListOfLines = []

    for i in range(len(listOfLines)):
        listOfWords = listOfLines[i].strip().split()
        newListOfWords = []
        newString = ""

        for i in range(len(listOfWords)):
            if listOfWords[i] in listOfSQLKeywords:
                newString += listOfWords[i].upper() + " "
            else:
                newString += listOfWords[i] + " "
        
        newString = newString.strip() + "\n"

        newListOfLines.append(newString)
        

    return newListOfLines

filename = input("filename: ") + ".sql"

infile = open(filename, "r")


newFile = convertToCapital(infile.readlines())
infile.close()
outfile = open(filename, "w")

outfile.writelines(newFile)
