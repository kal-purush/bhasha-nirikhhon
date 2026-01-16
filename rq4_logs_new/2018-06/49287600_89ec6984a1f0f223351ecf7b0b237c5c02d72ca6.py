#!/usr/bin/python 
"""
This script do get list from design out
usage:

example: python get_list_from_design_out.py resfinder.one.design.out
"""

import sys

def main():

    flag = 0
    for line in open(sys.argv[1],'r'):
        if line[:13] == "Sequences hit":
            flag = 1
            continue

        if flag == 1:
            if line[:3] == "---":
                flag = 0
                continue
            print line,
        

if __name__ == '__main__':
    main()