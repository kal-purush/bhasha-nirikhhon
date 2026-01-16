#!/usr/bin/python

# env is a command that manages the environment.
# it's probably not setup on edison, so use the explicit path  
#!/usr/bin/env python  

#These are dependencies  
import mraa  
import time
import sys
sys.path.append('/usr/lib/python2.7/site-packages')
import rrdtool

ret = rrdtool.create("photocell.rrd", "--step","300", "--start", "0","DS:photocell:GAUGE:330:U:U", "RRA:AVERAGE:0.5:1:600","RRA:AVERAGE:0.5:6:100")

#step is maximum interval between each value
#..GAUGE:310:U:U   '330 is the timeout value in seconds - if no data is added, zero value will be inserted in archive