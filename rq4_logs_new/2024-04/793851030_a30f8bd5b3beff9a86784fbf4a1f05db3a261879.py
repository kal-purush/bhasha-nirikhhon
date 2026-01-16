#!/bin/bash

import sys

# This input_line variable takes the number of shifts we want.
# The [1] means we take in 1 value
input_line=sys.argv[1]
print(input_line)

for line in sys.stdin:
    line = line.upper()
    print(line)