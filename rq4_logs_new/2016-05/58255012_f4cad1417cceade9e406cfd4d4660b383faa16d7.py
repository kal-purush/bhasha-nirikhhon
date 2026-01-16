##!/usr/bin/env python
# coding : utf-8
###########################
#author:   yuya aoki
#
###########################
import sys

def main():
    if len(sys.argv) == 3:
        print int(sys.argv[1])+int(sys.argv[2])
    else:
        print "err"

if __name__ == '__main__':
    main()