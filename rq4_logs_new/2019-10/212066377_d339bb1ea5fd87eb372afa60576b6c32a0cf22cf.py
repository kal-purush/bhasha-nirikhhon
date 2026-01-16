#!/usr/bin/python3
import sys

"""
reducer()
main reducer function
Outputs an integer of how many questions (PostTypeId = 1)
Outputs the number of questions that have at least 1 answer

input:
  None

returns:
  None, prints words into format acceptable by Hadoop
"""
def reducer():    
    total_average = 0

    for line in sys.stdin:
        count = line.split()[-1]

        total_average += int(count.strip())
    
    print(total_average)

reducer()