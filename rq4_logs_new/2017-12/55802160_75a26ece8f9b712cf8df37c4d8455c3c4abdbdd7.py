#!/usr/bin/python3
searchSearchString = "^...."#first 4 char in line

searchReplaceString = "foobar\n\r"
searchReplaceString = "\1"
collapseflag="left" # for doc insert
replaceAllflag="of1f"

incedent=5
insert="\njj\n" #string to be inserted for or appended to selection
appendflag=1


### spawn the  multiline selection to a uno-regex2
incedent2=2
searchSearchString2="la"

#the inserted TOC needs to be updated
TOCupdateflag=1

#howto lookup values: 1 macrorecorder, 2. python console readline completion >>> oCursor.ParaA 3. idl file grep

setPropertyValue1a="CharColor"
setPropertyValue1b=255

setPropertyValue2a="NumberingStyleName"
setPropertyValue2b="Outline"
 #second "index marks" Numbering continuing: "outline" else: Numbering 1

setPropertyValue3a="CharHeight"
setPropertyValue3b=8

#setPropertyValue4a="CharUnderline"
#setPropertyValue4b=1

setPropertyValue4a="ParaAdjust"
setPropertyValue4b=2

#LEFT    = 0RIGHT   = 1BLOCK   = 2CENTER  = 3TRETCH = 0

ParaStyleName="Heading 1"


STYLESFILE="/tmp/qemuburotest/Buildingdemon/b/g/test2.doc" # defunct?
#style families 5
#CharacterStyles NumberingStyles  ParagraphStyles  #FrameStyles PageStyles


ChapterFILE="/tmp/qemuburotest/Buildingdemon/b/g/test2.doc"#input#./Buildingdemon/b/g/test2.doc
logfile="/tmp/qemuburotest/logs"#o
logsline="hello_libreoffice-python"

absoluteUrlstr="/tmp/qemuburotest/Buildingdemon/b/e/i/"
#we got a seven files, in test: one out of another
absoluteUrltxt="/tmp/qemuburotest/Buildingdemon/b/e/i/test1.txt"
absoluteUrlpdf="/tmp/qemuburotest/Buildingdemon/b/e/i/test1.pdf"
loadComponentFromabsoluteURL="/tmp/qemuburotest/Buildingdemon/b/e/i/test1.doc" # 

filetype1a='writer_pdf_Export'
filetype2b=""

filetype2a='Text'
filetype2b=""

filetype3a='MS Word 97'
filetype3b="test2.doc"

filetype4a='writer8'
filetype4b="test2.odt"

#https://github.com/LibreOffice/core/blob/3168c5e09f084e65a32b5865d975ea9a9524d7c3/desktop/source/lib/init.cxx
 #{ "doc",   "MS Word 97" },
    #{ "docm",  "MS Word 2007 XML VBA" },
    #{ "docx",  "MS Word 2007 XML" },
    #{ "fodt",  "OpenDocument Text Flat XML" },
    #{ "html",  "HTML (StarWriter)" },
    #{ "odt",   "writer8" },
    #{ "ott",   "writer8_template" },
    #{ "pdf",   "writer_pdf_Export" },
    #{ "rtf",   "Rich Text Format" },
    #{ "txt",   "Text" },
    #{ "xhtml", "XHTML Writer File" },
    #{ "png",   "writer_png_Export" },


import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config_file')
parser.add_argument('-l', '--loadComponentFromabsoluteURL')
args = parser.parse_args()
if args.config_file!=None:
    pass
    config_file=args.config_file
    #print(config_file)
    with open(config_file) as f:
        code = compile(f.read(), config_file, 'exec')
        exec(code, globals(), locals())
if args.loadComponentFromabsoluteURL!=None:
    loadComponentFromabsoluteURL=args.loadComponentFromabsoluteURL
    pass




##https://stackoverflow.com/a/37611448 ". conf.sh"
#print(replaceAllflag)





#import /home/kubuntu/Downloads/format_config

import uno
import os
from com.sun.star.beans import PropertyValue
import subprocess
import time

##a launch server hack

a=""
try:
    a=subprocess.check_output('ps ax | grep soffice | grep -v "grep\|defunct" ',shell=True)
except:
    pass

print(str(a)+"asubprocess.check")
if (a==""):
    try:
        os.system('killall soffice.bin;')
    except:
        pass
    print("start soffice --accept")
    ##a=subprocess.check_output('soffice "--accept=socket,host=localhost,port=8100;urp;" --headless --invisible &',shell=False)
    os.system('soffice "--accept=socket,host=localhost,port=8100;urp;" --headless --invisible &')
#prove (if), killstart, while wait?
while (str(a)==""):    
    try:
        a=subprocess.check_output('netstat -nap | grep office ',shell=True)
    except:
        pass
    print(str(a)+"innen_subprocess.check")
    time.sleep(0.01)

#time.sleep(4.01)

def overwriteStyles(document, fromFile):
    """First arg is the document itself, the second is a string with path to style file in UNO API format"""
    styles = document.StyleFamilies
    styles.loadStylesFromURL(fromFile, styles.StyleLoaderOptions)

def updateTOC(document):
    """Update links in table of contents"""
    iList = document.getDocumentIndexes()
    assert(iList.getCount() > 0), "Table of contents not found, aborting!"
    for i in range(0, iList.getCount()):
        iList.getByIndex(i).update()

def setFirstPage(file):
    """Sets the first page style to «First Page»"""
    enumeration = file.Text.createEnumeration()
    enumeration.nextElement().PageDescName = 'First Page'



localContext = uno.getComponentContext()
resolver = localContext.ServiceManager.createInstanceWithContext(
                "com.sun.star.bridge.UnoUrlResolver", localContext )

smgr = resolver.resolve( "uno:socket,host=localhost,port=8100;urp;StarOffice.ServiceManager" )
remoteContext = smgr.getPropertyValue( "DefaultContext" )
desktop = smgr.createInstanceWithContext( "com.sun.star.frame.Desktop",remoteContext)

a="file:///"+ loadComponentFromabsoluteURL
file = desktop.loadComponentFromURL(a ,"_blank", 0, ())

#load a styte from another .doc file

if STYLESFILE!="":overwriteStyles(file, "file:///"+ STYLESFILE)


### uno-regex1, replaceAll first tier

search = file.createReplaceDescriptor()
search.SearchRegularExpression = True
search.SearchString = searchSearchString
search.ReplaceString = searchReplaceString
selsFound = file.findAll(search)
if replaceAllflag!="off":
    selsreplace =file.replaceAll(search)

#insert/ ChapterFILE/string before/after regex 1/regex1-2
oSel =selsFound.getByIndex(incedent)
oCursor = oSel.getText().createTextCursorByRange(oSel)


### spawn the  multiline selection to a uno-regex2
if searchSearchString2!="":
    search2 = file.createReplaceDescriptor()
    search2.SearchRegularExpression = True
    search2.SearchString = searchSearchString2
    search.ReplaceString = searchReplaceString
    selsFound2 = file.findAll(search2)
    oSel2=selsFound2.getByIndex(incedent2)
    oCursor.gotoRange(oSel2,True)

#inserted for or appended to selection
appendstr=''
if appendflag==1:
    appendstr=oCursor.getString()
oCursor.setString(insert+appendstr)
#oCursor.collapseToStart() #??

oCursor.setPropertyValue( setPropertyValue1a, setPropertyValue1b ) 

oCursor.ParaStyleName=ParaStyleName #toc things

oCursor.setPropertyValue(setPropertyValue2a,setPropertyValue2b)

oCursor.setPropertyValue(setPropertyValue3a,setPropertyValue3b)

oCursor.setPropertyValue(setPropertyValue4a,setPropertyValue4b)


# doc insert
if collapseflag=="left":
    oCursor.collapseToStart()
else:
    oCursor.collapseToEnd()

if ChapterFILE!="":
    oCursor.insertDocumentFromURL("file:///"+ ChapterFILE, ())

#tbi
#a=file.getScriptProvider()
#cmd = "Main?language=Basic&location=My Macros&module=Module1&methode=Standard"
#a.getScript("vnd.sun.star.script:"+cmd)##file.Text.getString()
#main__.ScriptFrameworkErrorException: The following Basic script could not be found:
#library: 'Main'
#module: ''
#method: ''
#location: 'My Macros'
#a.invoke()
#Traceback (most recent call last):
#  File "<stdin>", line 1, in <module>
#AttributeError: invoke

#paragraphing recently not implemented
#enumeration = file.Text.createEnumeration()
#par = enumeration.nextElement()

#TOC, tbi create, delete
if TOCupdateflag==1:
    updateTOC(file) 


sections = file.Links.Sections
for name in sections:
    sections[name].dispose()


#multiformated output, append/case

sections = file.Links.Sections
for name in sections:
    sections[name].dispose()

args = (PropertyValue('FilterName',0,'writer_pdf_Export',0),)
file.storeToURL("file://"+ absoluteUrlpdf,args)

argstxt = (PropertyValue('FilterName',0,'Text',0),)
file.storeToURL("file://"+ absoluteUrltxt,argstxt)

argstxt = (PropertyValue('FilterName',0,'MS Word 97',0),)
argsurl="file://"+ absoluteUrlstr+"test2.doc"
print(argsurl)
file.storeToURL(argsurl,argstxt)

args = (PropertyValue('FilterName',0,'writer8',0),)
argsurl="file://"+ absoluteUrlstr+"test2.odt"
file.storeToURL(argsurl,args)


file.dispose() #does what?

logshandle=open(logfile, 'a') #'a' opens the file for appending
logshandle.write(logsline)#'a' opens the file for appendingf.write(line)
logshandle.close() #'a' opens the file for appendingf.close()   