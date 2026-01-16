import os, sys

arguments = sys.argv[1:] # The first argument is actually the name of the script

if len(arguments) == 1: 
    print(os.path.getsize(arguments[0]))
else: 
    print(f'filesize requires exactly one argument! Got {len(arguments)}')
