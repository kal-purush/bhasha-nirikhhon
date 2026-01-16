#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Oct  3 12:44:51 2017

@author: horas
"""

read_time = open ('time2minute.ascii','r')
time_lines = read_time.readlines()
time_len = len (time_lines)
read_time.close()

read_disp = open ('BJI_displacement.ascii','r')
disp_lines = read_disp.readlines()
disp_len = len (disp_lines)
read_disp.close()

concatenate = open ('BJI_full.ascii','a+')
for i in range (time_len):
    disp_line = disp_lines[i]
    disp_line = float (disp_line)
    concatenate.write("%.26s\t%d\n"%(time_lines[i],disp_line))
    #concatenate.write("%"%)

concatenate.close()