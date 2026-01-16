import argparse
import sys
from ats import easypy


parser = argparse.ArgumentParser(description="Write a letter")
parser.add_argument('--letter', type=str, required=True)


def main():
    args, sys.argv[1:] = parser.parse_known_args(sys.argv[1:])
    print(args)
    easypy.run(testscript='tests.py',
               letter=args.letter)