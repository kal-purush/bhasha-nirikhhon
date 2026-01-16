#!/usr/bin/python
#coding: utf-8
#import sys
#import io
import cgi
import flashsqlfunc
import cgifunc

cgifunc.head('add')
print "<meta http-equiv=\"refresh\" content=\"0;URL=http://p06044.server-on.net/flashcard.py\">"
form = cgi.FieldStorage()
word = form["word"].value.replace('\'', '\'\'')
translate = form["translate"].value.replace('\'', '\'\'')
print word
print translate
cgifunc.end()
flashsqlfunc.additem(word, translate)