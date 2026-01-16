"""

@autor Thomas Pokorny

 ________  ___    ___ ________  _______   ________      
|\   __  \|\  \  /  /|\   ____\|\  ___ \ |\   ___  \    
\ \  \|\  \ \  \/  / | \  \___|\ \   __/|\ \  \\ \  \   
 \ \   ____\ \    / / \ \  \  __\ \  \_|/_\ \  \\ \  \  
  \ \  \___|\/  /  /   \ \  \|\  \ \  \_|\ \ \  \\ \  \ 
   \ \__\ __/  / /      \ \_______\ \_______\ \__\\ \__\
    \|__||\___/ /        \|_______|\|_______|\|__| \|__|
         \|___|/                                        
                                                        

@brief a probably completely useless tool that I wrote when I was bored instead of actually learning for Uni


"""

import json
import sys
import os

versionString = "pyGen version: 1.0"
helpString = "Calling Synopsis:\npyGen %path_to_json_file%"

#### SOME JAVA IDENTIFIERS

pubJ    = "public"
priJ    = "private"

classJ  = "class"
abstJ   = "abstract"
intJ    = "interface"
extenJ  = "extends"
implH   = "implements"

beginJ  = "{"
endJ    = "}"


def main():

    # the only arg. is the json file 
    args = sys.argv

    if len(args) <= 1:
        print(versionString+"\n"+helpString)
        raise SystemExit

    if args[1] == '-V':
        print(versionString)
        raise SystemExit
    try:
        f = open(args[1], 'r')
        javaGen = json.load(f)

        keys = javaGen.keys()
        # every key represents a class
        for c in keys:
            print(">> creating new Java Class: "+c+".java")
            createClass(c, javaGen[c])

        f.close()

    except FileNotFoundError:
        print('The given file does not exist!')

def createClass(className, values):
    # creating the file
    javaFile = open(className+".java","w+")

    accessModifier      = pubJ
    classIdentifier     = classJ # this can be either an "interface" or a "class"
    abstract            = "" # per default the class is not abstract 
    inheritClass        = ""
    compisitionClass    = ""
    extendsC            = False
    implemntsC          = False


    if "private" in values and values["private"] is True:
        accessModifier = pubJ
    if "abstract" in values and values["abstract"] is True:
        abstract = " " + abstJ
    if "interface" in values and values["interface"] is True:
        classIdentifier = intJ
    if "extends" in values:
        extendsC = True
        inheritClass = values["extends"]
    if "implements" in values:
        implemntsC = True
        compisitionClass = values["implements"]

    classString = accessModifier + abstract + " " + classIdentifier + " " + className 
    if extendsC is True:
        classString = classString + " " + extenJ + " " + inheritClass
    if implemntsC is True:
        classString = classString + " " + implH + " " + compisitionClass

    ## WRTIE TO GENERATED FILE ##
    javaFile.write(classString+"\r\n") 

    javaFile.write(beginJ+"\r\n")  
    # here some additional stuff could happen
    javaFile.write(endJ+"\r\n")

    javaFile.close() 

if __name__ == "__main__":
    main()