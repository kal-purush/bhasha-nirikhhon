from binascii import Incomplete
from enum import auto


f = open("Day10Input.txt", "r")

syntaxScore = 0
incompleteLines = []
for l in f:
    isCurrupted = False
    lineParser = []
    for c in l.strip():
        if c == '(' or c == '[' or c == '{' or c == '<':
            lineParser.append(c)
        elif c == ')' or c == ']' or c == '}' or c == '>':
            oChar = lineParser.pop()
            if c == ')':
                if oChar != '(':
                    #print("Expected " + oChar + " but found " + c + " instead")
                    syntaxScore += 3
                    isCurrupted = True
                    break
            elif c == ']':
                if oChar != '[':
                    #print("Expected " + oChar + " but found " + c + " instead")
                    syntaxScore += 57
                    isCurrupted = True
                    break
            elif c == '}':
                if oChar != '{':
                    #print("Expected " + oChar + " but found " + str(c) + " instead")
                    syntaxScore += 1197
                    isCurrupted = True
                    break
            elif c == '>':
                if oChar != '<':
                    #print("Expected " + oChar + " but found " + c + " instead")
                    syntaxScore += 25137
                    isCurrupted = True
                    break
            else:
                print("invalid character detected")
        if isCurrupted:
            break
    if not isCurrupted:
        incompleteLines.append(l.strip())


print("Part 1: " + str(syntaxScore))


#print(incompleteLines)
autocompleteScores = []

for l in incompleteLines:
    lineParser = []
    lineEnd = []
    l = l[::-1]
    for c in l:
        if c == '(' or c == '[' or c == '{' or c == '<':
            if lineParser == []:
                if c == '(':
                    lineEnd.append(')')
                if c == '[':
                    lineEnd.append(']')
                if c == '{':
                    lineEnd.append('}')
                if c == '<':
                    lineEnd.append('>')
            else:
                lineParser.pop()
        else:
            lineParser.append(c)

    scoreCounter = 0
    for c in lineEnd:
        scoreCounter *= 5
        if c == ')':
            scoreCounter += 1
        if c == ']':
            scoreCounter += 2
        if c == '}':
            scoreCounter += 3
        if c == '>':
            scoreCounter += 4
    autocompleteScores.append(scoreCounter)


autocompleteScores.sort()
#print(autocompleteScores)
print("Part 2: " + str(autocompleteScores[len(autocompleteScores)//2]))


