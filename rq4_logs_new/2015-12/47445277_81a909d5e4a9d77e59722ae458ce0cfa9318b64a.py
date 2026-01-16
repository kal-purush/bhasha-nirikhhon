#!/usr/bin/env python
import sys

for line in sys.stdin:
    line       = line.strip()   #strip out carriage return
    key_value  = line.split(",")   #split line, into key and value, returns a list

    print( '%s\t%s' % (key_value[0].strip(), key_value[1].strip()) );
    