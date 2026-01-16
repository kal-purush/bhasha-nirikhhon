

import time
import sys
import subprocess
import shlex

musicfile=sys.argv[1]
print "launching"
args = shlex.split('sudo python staller.py '+musicfile)
print args
subprocess.Popen(args)
time.sleep(0.01)
args = ['omxplayer',musicfile]
print args
subprocess.call(args)