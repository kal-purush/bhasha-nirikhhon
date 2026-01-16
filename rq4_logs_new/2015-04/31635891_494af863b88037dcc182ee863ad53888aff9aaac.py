#!/usr/bin/python

from __future__ import print_function
import sys # for path

# print something to see that we are ok...
print('here we are before scons...')

# /usr/lib/scons is where scons code is according to dpkg(1)
sys.path.insert(0, '/usr/lib/scons')

# now run scons
import SCons.Script
SCons.Script.main()