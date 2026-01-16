# coding: utf-8

import codecs
import re
from test import test_univnewlines
#ファイル読み出し
ifile = codecs.open('D:\hightemp.txt', 'r','utf-8')
lines=ifile.read()

def extractCol(lines):
    editCol=[]
    #1列目を抜き出す。最初の\tまでを抜き出す
    col=re.findall('^.*?\t', lines, re.MULTILINE)
    #\tを\nに置換して文字列化
    for i in range(len(col)):
        editCol.append(col[i].replace('\t','\n'))
        str="".join(editCol)
    return str
str1=extractCol(lines)
#1列目を取り除く
editedLines=re.sub('^.*?\t', '', lines,flags=re.MULTILINE)
str2=extractCol(editedLines)

#テキストファイルの書き出し
ofile=open('col1.txt', 'w')
ofile.write(str1)
ofile.close()
ofile=open('col2.txt', 'w')
ofile.write(str2)
ofile.close()