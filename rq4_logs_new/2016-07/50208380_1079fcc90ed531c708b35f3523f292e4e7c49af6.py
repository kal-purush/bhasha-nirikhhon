#!/usr/bin/env python

'''
Created on 2016/07/11

@author: maximilian
'''

import png
import argparse


if __name__ == '__main__':
    data = [(0,255,0,),
            (255,150,255),
            (0,255,0) ]
    test = open('test.png', 'wb')
    writer = png.Writer(3,3, greyscale=True)
    writer.write(test, data) ; test.close()
    
class Plotter:
    #z (start 1, ende zgrid), dann y(start -gridradius, ende +gridradius), dann x (wie y), dann t (start von vorne)
    #es laeuft also von -x,-y,1 nach -x,-y,z, dann -x, -y+1, 1 nach -x, -y+1, z ... -x, y, 1 nach -x, y, z, dann -x+1, -y, 1 nach -x+1, -y, z bis x, y, 1 nach x, y, z
    #"Steps output
    
    file = []
    data = []
    
    
    def __init__(self):
        options = argparse.ArgumentParser()
        options.add_argument("-f","--file", help="enter inputfile (path)")
        args = options.parse_args()
    
        print ("Starting program")
        source = args.file
        if source == None:
            source = raw_input("Enter path of inputfile:")
        print ("Reading inputfile...")
        try:                                                                    #inputfile einlesen
            self.file.append(open(source).readlines())
        except Exception:
            print ("Error while reading inputfile. File not existing?")
            raw_input("Press ENTER to exit program.")
            exit()
        print self.data
        
    
    

    def makeColorScale(self):
    