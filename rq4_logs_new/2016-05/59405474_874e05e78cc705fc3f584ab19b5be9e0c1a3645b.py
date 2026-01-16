import numpy as np
import argparse


# https://www.safaribooksonline.com/library/view/python-cookbook/0596001673/ch04s07.html
# http://docs.scipy.org/doc/numpy-1.10.1/reference/routines.random.html
# https://docs.python.org/2/library/stdtypes.html#bltin-file-objects
# https://docs.python.org/2/library/argparse.html?highlight=argparse#module-argparse

def select_cases(fileName, count=10):
    with open(fileName) as f:
        for i, line in enumerate(f):
            if i == 1:
                print line

select_cases("./all_pairs.txt")

np.random.randint(2, size=10)


count = 0
for line in open("./all_pairs.txt").xreadlines(  ): count += 1
    #
    #if __name__ == "__main__":
    #    parser = argparse.ArgumentParser()
    #    parser.add_argument("-p", "--port", type=int, default=11004, help="Agent mock port (default: 11004)")
    #    args = parser.parse_args()