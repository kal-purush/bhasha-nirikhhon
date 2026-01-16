#!/usr/bin/python

import urllib
import re

i = 365
while i> 330:
        url = "http://www.facets.la/2014/"+str(i)+"/"
        print url
        f = urllib.urlopen(url)
        print f
        hteml = f.read()
        print hteml
        myurl = re.compile('http:\S+W_\S+.jpg')
        print myurl
        if myurl.findall(hteml):
                imgurl = myurl.findall(hteml)[0]
                print imgurl
                filename = str(i)+".jpg"
                print filename
                urllib.urlretrieve(imgurl, filename)
        i = i-1

i = 330
while i> 0:
        url = "http://www.facets.la/2013/"+str(i)+"/"
        print url
        f = urllib.urlopen(url)
        print f
        hteml = f.read()
        print hteml
        myurl = re.compile('http:\S+W_\S+.jpg')
        print myurl
        if myurl.findall(hteml):
                imgurl = myurl.findall(hteml)[0]
                print imgurl
                filename = str(i)+".jpg"
                print filename
                urllib.urlretrieve(imgurl, filename)
        i = i-1