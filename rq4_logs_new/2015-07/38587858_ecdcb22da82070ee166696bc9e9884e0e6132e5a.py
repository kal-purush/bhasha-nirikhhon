#!/usr/bin/env python

# Generates test data sets for DICP models in a given directory.

import sys
sys.path.append('.')

from dicp import Problem
import os

if __name__ == '__main__':
    try:
        outdir = sys.argv[1]
    except IndexError:
        print 'usage: %s output-directory' % sys.argv[0]
        sys.exit(1)

    # TODO: configure this from the command line.
    for num_images in range(5, 51, 5):
        for num_commands in range(25, 101, 25):
            max_time = 100
            probdir = os.path.sep.join([
                outdir,
                '%03dimages-%03dcmds' % (num_images, num_commands)
            ])
            os.mkdir(probdir)

            prob = Problem.generate(num_images, num_commands, max_time)
            prob.save(os.path.sep.join([probdir, 'input.json']))