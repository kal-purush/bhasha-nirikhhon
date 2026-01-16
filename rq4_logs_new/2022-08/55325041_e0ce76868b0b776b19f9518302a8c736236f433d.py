#!/usr/bin/env python
#---------------------------------------------------------------------------------------------------
# Make a user directory listing and produce output.
#
#                                                                             Ch.Paus (Sep 30, 2015)
#---------------------------------------------------------------------------------------------------
import os,sys,re,socket,datetime,time

#---------------------------------------------------------------------------------------------------
#  H E L P E R S 
#---------------------------------------------------------------------------------------------------
def showSetup(firstTime,fHandle):
    if firstTime:
        fileH.write("\n=-=-=-= Show who and where we are =-=-=-=\n\n")
        fileH.write(" Script        : %s\n"%(os.path.basename(__file__)))
        fileH.write(" Arguments     : %s\n"%(" ".join(sys.argv[1:])))
        fileH.write(" \n")
        fileH.write(" start time    : %s\n"%(str(datetime.datetime.now())))
    else:
        fileH.write(" time now      : %s\n"%(str(datetime.datetime.now())))

    fileH.write(" user executing: " + os.getenv('USER','unknown user') + "\n")
    fileH.write(" running on    : %s\n"%(socket.gethostname()))
    fileH.write(" executing in  : %s\n"%(os.getcwd()))
    fileH.write(" arguments     : %s\n"%(" ".join(sys.argv[1:])))
    fileH.write(" \n")

    return

def showSetupStd():
    print(" user executing: " + os.getenv('USER','unknown user'))
    print(" running on    : %s"%(socket.gethostname()))
    print(" executing in  : %s"%(os.getcwd()))
    print(" user list     : %s"%(" ".join(sys.argv[1:])))
    print(" time now      : %s"%(str(datetime.datetime.now())))
    print(" ")

    return

#---------------------------------------------------------------------------------------------------
#  M A I N
#---------------------------------------------------------------------------------------------------

username = sys.argv[1]

# make announcement
#showSetupStd()
cmd = 'echo " listing /data/submit/%s:"; ls -l /data/submit/%s'%(username,username)
os.system(cmd)

sys.exit(0)