#!/bin/python3

import math
import os
import random
import re
import sys



#
# Complete the 'getMaxAggregateTemperatureChange' function below.
#
# The function is expected to return a LONG_INTEGER.
# The function accepts INTEGER_ARRAY tempChange as parameter.
#

def getMaxAggregateTemperatureChange(tempChange):
        # Write your code here
        ########################
        value=tempChange[0]
    for i in range(len(tempChange)):
        value=max(max(sum(tempChange[:i+1]),sum(tempChange[i:])),value)
    return(value)
######################################################################
if name == 'main':
    fptr = open(os.environ['OUTPUT_PATH'], 'w')

    tempChange_count = int(input().strip())

    tempChange = []

    for _ in range(tempChange_count):
        tempChange_item = int(input().strip())
        tempChange.append(tempChange_item)

    result = getMaxAggregateTemperatureChange(tempChange)

    fptr.write(str(result) + '\n')

    fptr.close()