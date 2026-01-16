# Attribute Export
# Samuel Volk, 2020
#
# Converts a binary file of compiled gameboy assembly into mush attributes.
#

import argparse
from itertools import zip_longest
from binascii import hexlify

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


parser = argparse.ArgumentParser(description="Convert compiled gb assembly to mush attributes.")
parser.add_argument('file', help='specify input file')
args = parser.parse_args()

outfilename = args.file[:-4] + '.mush'
outfile = open(outfilename, 'w')

f = open(args.file, 'rb')
with open(args.file, 'rb') as f:
    data = bytearray(f.read())

    #print (list(grouper(data, 256, 0)))
    for idx,chunk in enumerate(grouper(data, 256, 0)):
        outfile.write('&MEM-' + str(idx) + ' zei =' + ' '.join(map(str,chunk)) + '\n')