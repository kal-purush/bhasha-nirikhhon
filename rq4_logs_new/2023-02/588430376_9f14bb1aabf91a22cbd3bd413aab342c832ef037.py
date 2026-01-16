# Carly Watkins
# 10/31/22
# CS-100A - Final project
# License: Copyright 2022 Carly Watkins
# Resources used: https://google.github.io/styleguide/pyguide.html


from createbeds1 import bed 
from datetime import date, timedelta, datetime
from userinteractiontest import PlantWitch, QWidget

import json

def getuserresponse(beds):
    QN = self.txtbed.text() 
    while QN != 'done':
        QL = self.txtLength.text()
        QW = self.txtwidth.text()
        QD= self.txtdepth.text()
        QS= self.txtsun.text()
        QDr= self.txtdrain.text()
        Qffd= self.txtffd.text()
        Qpfd= self.txtpfd.text()
        Qfinfd= self.txtfinfd.text()
   
       
        beds[QN]=bed(QN, QL, QW, QD, QS, QDr, Qffd, Qpfd, Qfinfd)
        
        QN = self.lblbed.text() 

        #If I enter a new bed name instead of entering "done" here, it will write both to the file, but as a single line. 