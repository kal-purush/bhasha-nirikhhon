#!/usr/bin/env python
import random
import math
from optparse import OptionParser

RANDOM_SEED = 42
REPAIR_MEAN = 450.0 * 60    	# Avg. repair time in seconds
REPAIR_SIGMA = 4000.0        	# Sigma of processing time
MTTF = 30.0 * 60 * 60  		# Mean time to failure in seconds
BREAK_MEAN = 1 / MTTF  		# Param. for expovariate distribution
WEEKS = 120             	# Simulation time in weeks
SIM_TIME = WEEKS * 7 * 24 * 60 * 60 	# Simulation time in seconds
MAINTAIN_GAP = 7 * 24 * 60 * 60
MAINTAIN_DURATION = 8 * 60 * 60 


joblist = {}
replicalist = []
tasklist = []
nodelist = []
clusterlist = []

def test(env):
	for i in range(1,10,1):
		t = 10 * i - env.now
		yield env.timeout(t)
		print "event time: %d" % env.now
		yield env.timeout(3)
		print "event end: %d" % env.now

if __name__ == "__main__":
    p = OptionParser()
    p.add_option("-t", "--simtime", dest = "simtime", type = "int", 
                    help = "length of simulation time in weeks") 
    
    (opts, args) = p.parse_args()
    
    if not opts.joblog:
        print "please specify path of job log file (-j)"
        p.print_help()
        exit()
    

    if opts.outputlog:
    	OUTPUT = opts.outputlog

    if opts.simtime:
    	SIM_TIME = opts.simtime * 7 * 24 * 60 * 60

    random.seed(RANDOM_SEED)