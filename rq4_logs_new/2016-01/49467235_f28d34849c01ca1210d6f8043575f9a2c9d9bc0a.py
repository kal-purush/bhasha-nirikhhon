#!/usr/bin/python

import os
import sys
import commands
import re

if len(sys.argv) == 2:
    f = open(sys.argv[1] + '.sh','w')
    output = "#!/bin/bash\n\n"
    f.write(output)
    for root, dirs, files in os.walk(sys.argv[1]):
        cmd1 = "ls -d --full-time \"" + root + "\""
        cmd1rtnval = commands.getoutput(cmd1)
        match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{9} \+\d{4}', cmd1rtnval)
        output = "touch -d \"" + match.group() + "\" \"" + root + "\"\n"
        f.write(output)
    f.close()
    cmd2 = "chmod 755 " + sys.argv[1] + ".sh"
    rtnval = commands.getoutput(cmd2)

    # migrate your OSTx
    cmd3 = "lfs find -obd lustre-OST0002 " + sys.argv[1] + " | lfs_migrate -y"
    rtnval = commands.getoutput(cmd3)
    cmd4 = "lfs find -obd lustre-OST0003 " + sys.argv[1] + " | lfs_migrate -y"
    rtnval = commands.getoutput(cmd4)

    cmd5 = "sh " + sys.argv[1] + ".sh"
    rtnval = commands.getoutput(cmd5)
    cmd6 = "rm " + sys.argv[1] + ".sh"
    rtnval = commands.getoutput(cmd6)
else:
    print "usage: " + sys.argv[0] + " target_directory\n"