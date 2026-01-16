#!/usr/bin/python
'''
Utility to run analysis pipeline from command line.
We submit it to slurm.
'''
import sys
from HCtool.parallel import select_pipeline
print 'Path to cfg:', str(sys.argv[1])
select_pipeline(sys.argv[1])