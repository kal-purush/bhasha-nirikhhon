#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import os
os.system("pip install --upgrade textblob")
import textblob
from textblob import TextBlob
os.system("python -m textblob.download_corpora")
from sys import version_info



def infoExtract():
	py3 = version_info[0] > 2 #creates boolean value for test that Python major version > 2
	text = ""
	while(len(text) == 0):
		if py3:
			text = input("enter 'stop' to stop the program. please enter your question here: ")
		else:
			text = raw_input("enter 'stop' to stop the program. please enter your question here: ")

	text = text.lower()
	tb = TextBlob(text)
	if(text == "stop"):
		return "stop"
	sent_list = text.split(' ')
	thisType = ""
	types = ("what", "who", "when", "why", "how", "where")

	if(len(sent_list) != 0 and sent_list[0] in types):
	    thisType = sent_list[0]
	else:
	    for word in sent_list:
	        if(word in types):
	            thisType = word
	keyword = tb.tags
	result = ""
	for tuples in keyword:

		if(tuples[1] in ["NN", "NNP", "NNS", "NNPS", "JJ"]):

			result += "".join(tuples[0])+" "

	output = []
	output.append(thisType)
	output.append(result)
	output.append(text)
	print("Question Type: " + output[0] + " KeyInfo: " + output[1])
	return output