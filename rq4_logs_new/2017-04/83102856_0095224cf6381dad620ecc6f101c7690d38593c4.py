import os
#import requests
import sys, urllib2, urllib



comp_err_file = open("compile.e", 'r')
comp_err_str = comp_err_file.read()
comp_out_file = open("compile.o", 'r')
comp_out_str = comp_out_file.read()
fileName = str(sys.argv[1])
print 'something'

data = urllib.urlencode({'fileName':fileName,'compileO':comp_out_str, 'compileE':comp_err_str})
req = urllib.urlopen("http://10.194.47.139:8000/COL380/API/Compile/", data)
#response = urllib.urlopen(req)

os.system("/opt/pbs/default/bin/qsub -P cse -N Test1 -lselect=1:ncpus=21:mem=24gb -l walltime=00:15:00 run1.sh")
# os.system("/opt/pbs/default/bin/qsub -P cse -N Test2 -lselect=1:ncpus=21:mem=24gb -l walltime=00:15:00 run2.sh")
# os.system("/opt/pbs/default/bin/qsub -P cse -N Test3 -lselect=1:ncpus=21:mem=24gb -l walltime=00:15:00 run3.sh")
# os.system("/opt/pbs/default/bin/qsub -P cse -N Test4 -lselect=1:ncpus=21:mem=24gb -l walltime=00:15:00 run4.sh")
# os.system("/opt/pbs/default/bin/qsub -P cse -N Test5 -lselect=1:ncpus=21:mem=24gb -l walltime=00:15:00 run5.sh")
os.system("/opt/pbs/default/bin/qsub -P cse -N sendStatus -lselect=1:ncpus=21:mem=24gb -l walltime=00:15:00 SendStatus.sh")