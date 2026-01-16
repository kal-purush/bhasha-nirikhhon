import argparse
import sys
from ats import easypy


parser = argparse.ArgumentParser(description='just test for copying files')
parser.add_argument('--vm_username', type=str, required=True)
parser.add_argument('--host_username', type=str, required=True)
parser.add_argument('--file_name', type=str, required=True)


def main():
    args, sys.argv[1:] = parser.parse_known_args(sys.argv[1:])
    print(args)
    easypy.run(testscript='task_3.py',
               vm_username=args.vm_username,
               host_username=args.host_username,
               file_name=args.file_name)