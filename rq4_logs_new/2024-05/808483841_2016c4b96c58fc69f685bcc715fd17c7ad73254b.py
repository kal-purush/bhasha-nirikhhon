from persistence import *

import sys


def main(args: list[str]):
    inputfilename: str = args[1]
    with open(inputfilename) as inputfile:
        for line in inputfile:
            splittedline: list[str] = line.strip().split(", ")
            if int(splittedline[1]) > 0:
                repo.buy(splittedline)
            else:
                repo.sell(splittedline)


if __name__ == '__main__':
    main(sys.argv)