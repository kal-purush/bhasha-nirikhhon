#! /usr/bin/env python
#
# Created by: Brenden Bickner
# checks slack channel for latest ip and adds it to hosts file if needed

import sys
import subprocess
from optparse import OptionParser
from slacker import Slacker



def main():

    #### COLOURS ####
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    VARS_FILEPATH = '/home/brenden/.ipchecker.vars'
    SLACK_API_TOKEN = 'xoxb-282920378854-fo83cWTlYsXXjhFXR9Zqu8Xe'


    #### OPTION MENU ####
    usage = "./ip_checker.py [options]"
    parser = OptionParser(usage=usage)


    slack = Slacker(SLACK_API_TOKEN)

    messages= slack.channels.history(channel="C89BF8GF2",count=1).body
    current_ip = messages['messages'][0]['text'].rstrip()

    try:
        vars_file = open(VARS_FILEPATH,'r')
        the_vars = vars_file.read().split('\n')
        old_ip = the_vars[0]
    except IOError:
        print "ERROR READING FROM VARS FILE"


    if current_ip != old_ip:
        try:
            subprocess.Popen(
                "> " + VARS_FILEPATH,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True).communicate()

            vars_file = open(VARS_FILEPATH,'w')
            vars_file.write("%s\n" %current_ip)
            vars_file.close()
        except IOError:
            print "ERROR WRITING TO VARS FILE"
            return 1
            
        old_line = "%s" %old_ip
        new_line = "%s" %current_ip

        sed_command = "sed -i 's/%s/%s/g' /etc/hosts" %(old_line, new_line)
        subprocess.Popen(
            sed_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True).communicate()
    else:
        print "Doing Nothing"
        
    print("Current IP Address:  %s" %current_ip)
    print("Previous IP Address: %s" %old_ip)   
 

    return 0


#############################
if __name__ == "__main__":
    sys.exit(main())
#############################