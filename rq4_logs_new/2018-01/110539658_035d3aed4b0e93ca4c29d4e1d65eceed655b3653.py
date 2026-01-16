# Jocelyn Vernay
# M2 Data & Knowledge
# Web Data Models - Project : DTD Validator for XML
# October 17th, 2017

import validator as vd
import time
import random

# Handy structure to calculate the processing time
class TimeKeeper:
    def __init__(self):
        self.t = None
        
    def tic(self):
        self.t = time.time()
        return self.t
    
    def toc(self):
        return time.time() - self.t
    
# This will follow the following dtd :
# a b(c)*d
# b _
# c a
# d _

# example of valid xml document : 
# 0 a
# 0 b
# 1 b
# 0 c
# 0 a
# 0 b
# 1 b
# 0 d
# 1 d
# 1 a
# 1 c
# 0 d
# 1 d
# 1 a

# The resulting document may not be valid, however it is the computation performance that interests us
# This function is unfinished. It was supposed to be used to generate
# data to plot the different metrics.
def buildLinearly(name, nbchildren, spread, depth):
    lines = []
    
    nextElement = name
    nestedElements = []
    unfinishedElements = []
    
    random.seed()
    
    while 1:
        # introductory a
        if name == 'a':
            if nbchildren < 2: # a can't have less than two children
                break

            lines.append("0 a")
            nestedElements.append("a")
            nextElement = 'b'
            unfinishedElements.append([nbchildren - 1, spread, depth - 1, nextElement])
            nbchildren = round(nbchildren * spread)
            depth = depth - 1
            
        # wilcard backtrack
        elif nextElement == '_': 
            if (len(nestedElements) < 1):
                break
            
            hero = nestedElements[len(nestedElements) - 1]
            stats = unfinishedElements[len(unfinishedElements) - 1]
            
            nbchildren = stats[0]
            depth = stats[2]
            
            if hero == 'a':
                if stats[0] < 2:
                    break
                elif stats[3] == 'b' or stats[3] == 'c':
                    if random.random() > 0.7:
                        nextElement = 'c'
                    else:
                        nextElement = 'd'
                    nbchildren = round(nbchildren * spread)
                    depth = depth - 1
                elif stats[3] == 'd':
                    lines.append("1 a")
                    nestedElements.pop()
                    unfinishedElements.pop()
                    nextElement = '_'
            
            elif hero == 'c':
                lines.append("1 c")
                nestedElements.pop()
                unfinishedElements.pop()
                nextElement = '_'
            
        # introductory c
        elif nextElement == 'c':
            if nbchildren < 4: # c->a
                break

            lines.append("0 c")
            nextElement = 'a'
            
            nestedElements.append('c')
            unfinishedElements.append([nbchildren - 1, spread, depth - 1, nextElement])
            
            nbchildren = round(nbchildren * spread)
            depth = depth - 1
            
        elif len(nextElement) == 0:
            break
        else:
            lines.append("0 " + name)
            lines.append("1 " + name)
            nextElement = '_'
            
    while (len(nestedElements) and len(unfinishedElements)):
        print(nestedElements[len(nestedElements)] + " 1")
        nestedElements.pop()
        unfinishedElements.pop()
    return lines

# 'nbchildren' is the starting number of children per node
# 'spread' is a value around 1 indicating how much the tree will spread
# 'depth' is the depth that the tree will reach.
def generateXMLDocument(nbchildren, spread, depth):
    lines = []
    if depth > 0:
        lines.extend(buildLinearly('a', nbchildren, spread, depth))
    else:
        lines.append("0 a")
        lines.append("0 b")
        lines.append("1 b")
        lines.append("0 d")
        lines.append("1 d")
        lines.append("1 a")
    return lines
    
if __name__ == "__main__":
    
    #xml = generateXMLDocument(5, 0.8, 5)
    
    #print(xml)
    
    tk = TimeKeeper()
    
    # parse experiment.dtd
    dtd = vd.getLines("experiment.dtd")
    
    finalResults = []
    
    aResults = []
    # test with varying nbchildren :
    for i in range(2, 50):
        xml = generateXMLDocument(i, 1.25, 10)
        tk.tic()
        aResults.append((vd.validateXMLwithDTDfromLines(xml, dtd), tk.toc()))
        
    finalResults.append(aResults)
    
    # varying spread factor
    aResults = []
    for i in range(10):
        xml = generateXMLDocument(5, 0.5 + 0.15 * i, 10)
        tk.tic()
        aResults.append((vd.validateXMLwithDTDfromLines(xml, dtd), tk.toc()))
        
    finalResults.append(aResults)
    
    # varying depth
    aResults = []
    for i in range(5, 50):
        xml = generateXMLDocument(5, 1, i)
        tk.tic()
        aResults.append((vd.validateXMLwithDTDfromLines(xml, dtd), tk.toc()))
        
    finalResults.append(aResults)
    
    print(finalResults)