#!/usr/bin/env python
import csv
import sys
import random
import math
from random import shuffle
from TargetFile import TargetFile

class Acuity(TargetFile):

    def __init__(self):

        plateInfo = TargetFile('PlateInfo.txt')
        self.plateangle = []
        for row in plateInfo.tgt:
            self.plateangle.append(int(row[1]))

        self.plate = ['l','r']
        self.plateNum = [1,2]
        self.UL = max(self.plateangle)
        self.LL = min(self.plateangle)
        self.trgt = (self.UL - self.LL)/2
        self.score = 0
        self.index = 1
        self.trial()
    #accept file for plate and for hand
    def trial(self):
        Nrepeats =  11
        mylist = []
        file = raw_input('Enter subject ID [s##]:')
        f = raw_input('Enter block ID:')
        id = file
        blockid = f
        #self.toFile(mylist, id)
        handInfo = TargetFile('HandInfo.txt')
        for bn, hand, digit in handInfo.tgt:
            handflag = hand
            digitflag = digit
            print "Intruct subject to place hand: %s , digit: %s" %(handflag, digitflag)

            while ((self.UL-self.LL) > 2):
                for i in range(1,Nrepeats):

                    while True:
                        try:

                            value = self.trgt
                            val = [0, value]
                            leftplate = random.choice(val)
                            rightplate = random.choice(val)
                            if leftplate == rightplate:
                                continue
                            plate2 = [leftplate,rightplate]
                            print 'Trial #: %s' %(self.index) + ' Left Plate: %s Right Plate: %s' %(leftplate, rightplate)
                            plate_map = dict(zip(plate2,self.plate))
                            combo_map = dict(zip(self.plateNum, self.plate))
                            input = raw_input("Enter response [L/R]:")
                            input = input.lower()

                            if input == 'l' or input == 'r':
                                mylist.append(handflag)
                                mylist.append(digitflag)
                                mylist.append(bn)
                                mylist.append(self.index)
                                mylist.append(leftplate)
                                mylist.append(rightplate)
                                if input == 'l':
                                    for key in combo_map:
                                        if key == 1:
                                            mylist.append(key)
                                        else:
                                            continue
                                elif input == 'r':
                                    for key in combo_map:
                                        if key == 2:
                                            mylist.append(key)
                                        else:
                                            continue

                            else:
                                print "Enter either 'L' or 'R' "
                                continue

                            if input == plate_map.get(value,"None"):
                                mylist.append(1)
                                self.score += 1
                                self.index += 1
                            else:
                                mylist.append(0)
                                self.index += 1

                        except NameError:
                            print "Enter either 'L' or 'R' "
                            continue
                        else:
                            break


                if (self.score)>=6:
                    self.UL = self.trgt
                    v = (self.trgt-((self.UL-self.LL)/2))
                    self.trgt = math.ceil(v/2.)*2
                    self.score = 0

                else:
                    self.LL = self.trgt
                    v = (((self.UL- self.LL)/2)+ self.trgt)
                    self.trgt = math.ceil(v/2.)*2
                    self.score = 0

            self.UL = max(self.plateangle)
            self.LL = min(self.plateangle)
            self.trgt = (self.UL - self.LL)/2
            self.index = 1

        mylist[::-1]
        self.toFile(mylist, id,blockid)


    def next(self):
        self.trial()

    def toFile(self, mylist, id, blockid):
        with open(id +'_'+ blockid +'.txt', 'a') as f:
            writer = csv.writer(f,delimiter = '\t')
            composite_list = [mylist[x:x+8] for x in range(0, len(mylist),8)]
            writer.writerow(['Hand', 'Digit','BN', 'TN','LeftPlate','RightPlate','Response','Correct'])
            writer.writerows(composite_list)

