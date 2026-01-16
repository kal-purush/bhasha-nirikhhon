#!/usr/bin/python

import re, subprocess

f = open('wordlist.txt','r')
w = f.read()
w = w.split('\n')
for i in w:
    subprocess.call(["./binaary50", i]);