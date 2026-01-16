#!usr/bin/env python
import sys
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
#from prompt_toolkit.completion import WordCompleter

#Dic_completer = WordCompleter(["search","exit","save"], ignore_case=False)
dic_source = ""
while True:
     user_input = prompt("SEARCH WORD>", history=FileHistory("history.txt"),
     auto_suggest=AutoSuggestFromHistory(), )
     if user_input == "...":
         break
     else:
         print ("YOUR WORD:"+user_input)
         my_file = open("DicLib.txt", "r")
         dic_source = my_file.readline()
         while dic_source != "":
             if user_input == dic_source.strip("\n"):
                 dic_source = my_file.readline()
                 print (dic_source)
                 my_file.close()  
                 break
             else:
                 dic_source = my_file.readline()
         if dic_source == "":
             print("can't be found!")
             my_file.close()      