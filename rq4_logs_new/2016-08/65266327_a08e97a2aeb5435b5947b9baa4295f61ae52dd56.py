#! usr/bin/python
# Adapted from Stack Overflow

import sys
import shutil
import glob

outfilename = sys.argv[1]
directory = sys.argv[2]


with open(outfilename, 'wb') as outfile:
    for filename in glob.glob(str(directory) + '/*.fastq'):
        if filename == outfilename:
            # don't want to copy the output into the output
            continue
        with open(filename, 'rb') as readfile:
            shutil.copyfileobj(readfile, outfile)