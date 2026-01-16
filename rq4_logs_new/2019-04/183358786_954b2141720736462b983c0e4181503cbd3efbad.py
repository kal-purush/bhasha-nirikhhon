#!/usr/bin/env python

import subprocess
from datetime import datetime, date, time
import logging

current_wan = None
new_wan = None
cmd = "dig TXT +short o-o.myaddr.l.google.com @ns1.google.com | sed 's/\"//g'"
current_time = datetime.now().__format__("%a %m-%d-%y %I:%M:%S %p")
file_handle = open("wan-ip-log.txt", "a+")
logging.basicConfig(filename="check-wan.log",level=logging.DEBUG)

def main():
    new = getNewWan()
    current = getCurrentWan()
    compareWans(new, current)

#get new wan ip from google dns
def getNewWan():    
    new_wan = subprocess.check_output(cmd, shell=True).strip()
    return new_wan

#get current wan from file
def getCurrentWan():
    file_line_list = file_handle.readlines()
    try:
        current_wan = file_line_list[-1]
        ip, date = current_wan.split(",")
        return ip
    except IndexError as error:
        logging.exception(error)
        logging.error("No WAN found in file, ending script")

#compare new wan to current wan, skip write if the same
def compareWans(new_wan, current_wan):    
    if(new_wan == current_wan):
        logging.info("Wan IP did not change, ending script")
        print("new_wan: %s, current_wan: %s" % (new_wan, current_wan))
    else:
        file_handle.write("%s, %s\n" %(new_wan, current_time))
        logging.info("New Wan IP detected, logging to file")    
    file_handle.close()

main()
exit()


