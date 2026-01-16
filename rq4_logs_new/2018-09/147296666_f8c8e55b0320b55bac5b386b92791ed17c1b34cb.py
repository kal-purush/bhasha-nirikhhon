#!/usr/bin/env python3
from boa.compiler import Compiler
import getopt, sys

filename = None
runmode = 0

if __name__ ==  '__main__':
    opts, args = getopt.getopt(sys.argv[1:],"n:m:")
    for op, value in opts:
        if op == "-n":
            filename = value
        if op == "-m":
            runmode = int(value)

    if filename == None:
        print("Filename do not set!!!")
        exit()

    if runmode == 1:
        print("Runmode 1. Compile file", filename)
        Compiler.load_and_save(filename)
    elif runmode == 0:
        print("Runmode 0. Dump avm code message of file", filename)
        module = Compiler.load(filename).default
        module.write()
        module.to_s()