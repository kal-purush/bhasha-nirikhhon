#!/usr/bin/python
# Experimental parser

import argh

def node():
    pass

def service():
    pass

def reinit():
    pass

parser = argh.ArghParser()
parser.add_commands([reinit, node, service])

if __name__ == '__main__':
    parser.dispatch()
    