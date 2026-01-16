#!/usr/bin/env python

import getpass
import sys
import telnetlib


    tn.write("conf t\n")
    tn.write("bann motd #Warning,UNAUTHORISED USE MAY SUBJECT YOU TO CRIMINAL PROSECUTION#\n")
    tn.write("router eigrp\n")
    tn.write("network 0.0.0.0 0.0.0.0\n")
    tn.write("end\n")
    tn.write("exit\n")

    print tn.read_all()