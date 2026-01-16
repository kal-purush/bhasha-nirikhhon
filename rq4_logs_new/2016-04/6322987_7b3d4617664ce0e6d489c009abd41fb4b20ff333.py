from __future__ import print_function

import subprocess
import sys

def create_group(group, gid):
    cmd = 'groupadd'
    cmd_call = [cmd, '-g {0}'.format(gid), group]
    print('Creating new group "{0}", with gid = {1}'.format(group, gid))
    subprocess.call(cmd_call)
    

if len(sys.argv) > 1:
    group_file = sys.argv[1]
    print("Using file {0} as input".format(group_file))
    with open(group_file) as f:
        for line in f.readlines():
            group, gid = line.strip().split(':')
            create_group(group, gid)
else:
    print("Error: no group file specified", file=sys.stderr)