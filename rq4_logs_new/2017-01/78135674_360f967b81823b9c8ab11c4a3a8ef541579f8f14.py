#! /usr/bin/env python

def getColorCode(color):
    if color == 'green':
        return 92
    if color == 'red':
        return 91
    if color == 'yellow':
        return 93
    return 99



def colorString(color, string):
    colorCode = getColorCode(color)
    return "\033[{}m{}\033[0m".format(colorCode, string)



def printColor(color, string):
    print colorString(color, string)

