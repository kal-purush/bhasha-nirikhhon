from random import randint

FILE_NAME : str = "possibleFile1"
dimensions = (10, 10) # Line Count, Line Size (in terms of numbers)
injectLineSize = 20



def makeNumbers(size : int = dimensions[1]) -> list:
    numData : list = list()
    for i in range(size):
        numData.append(randint(0, 255))
    
    return numData

def corruptLine(numData : list, injected : str) -> str:
    line : str = ""
    dataSize : int = len(numData)
    injectSize = len(injected)
    injectPosition = 0
    for dataIdx in range(dataSize):
        maxPossibleNumInsert = injectSize - injectPosition - (dataSize - dataIdx - 1)
        numCharInsert = 1 
        if maxPossibleNumInsert > 1:
            numCharInsert = randint(1 if dataIdx > 0 else 0, maxPossibleNumInsert)
        line += injected[injectPosition : injectPosition + numCharInsert + 1] 
        line += str(numData[dataIdx])
        injectPosition += injectPosition + 1
    line += injected[injectPosition:]
    return line

def makeInjectionStr(size : int = injectLineSize) -> str:
    line : str = ""
    for i in range(size):
        ch : str = chr(randint(0, 255))
        while ch.isnumeric() or ch == '\n':
            ch : str = chr(randint(0, 255))
        
        line += ch
    return line 

# MAIN
with open(FILE_NAME + ".txt", "w", encoding = "utf-8") as file, open(FILE_NAME + "ANSWER.txt", "w", encoding = "utf-8") as fileAns:
    for lineCount in range(dimensions[0]):
        numData  = makeNumbers() 
        injected = makeInjectionStr()
        line     = corruptLine(numData, injected) + "\n"

        for num in numData:
            fileAns.write(str(num) +" ")
        fileAns.write("\n")

        file.write(line)

        

# with open("singleWithDigits.txt", "w", encoding = "utf-8") as file:
#     line : str = ""
#     for j in range(1):
        
#         for i in range(255):
#             charASCII = i
            
#             if (charASCII == ord("\n") or (charASCII >= ord("0") and charASCII <= ord("9"))):
#                 continue
            
#             line += chr(charASCII)
#             if (randint(0, 3) == 0):
#                 line += str(randint(0, 255))
#         line += '\n'
#     file.write(line)
