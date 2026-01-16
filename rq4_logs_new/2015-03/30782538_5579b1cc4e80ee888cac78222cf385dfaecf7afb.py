#!/usr/bin/python

# 27 February 2015 by Philpot

import sys
# import requests
import re
import subprocess

ESDTPROTOCOL='https'
ESDTHOST='esc.memexproxy.com'
# ESDTPORT=443
ESDTPATH='_cat/indices'
ESDTUSER='darpamemex'
ESDTPASSWORD='darpamemex'
VERBOSE=True

"""[studio monitor]$ curl -k -s "https://darpamemex:darpamemex@esc.memexproxy.com/_cat/indices"
green open .marvel-2015.02.28       1 1     22258       0 232.1mb 115.9mb 
green open dig-ht-pilot-unfiltered 40 1 503462558 9850045 675.8gb 337.9gb 
green open .marvel-2015.02.24       1 1    232683       0   1.4gb 739.9mb 
green open .marvel-2015.02.26       1 1   8218137       0  18.6gb   9.3gb 
green open .marvel-2015.02.27       1 1   3428004       0  10.4gb   5.2gb 
green open .marvel-2015.02.25       1 1   2340956       0   5.1gb   2.5gb 
green open .marvel-kibana           1 1         1       0   6.5kb   3.2kb 
"""

error = False
try:
    msg = None
    problems = []
    uri = "%s://%s:%s@%s/%s" % (ESDTPROTOCOL, ESDTUSER, ESDTPASSWORD, ESDTHOST, ESDTPATH)
    cmd = ["/usr/bin/curl", "-s", "-k", "%s" % uri]
    print cmd
    output = subprocess.check_output(cmd)
    for line in output.split('\n'):
        fields = re.split(r'''\s+''', line)
        if fields[0] == 'red':
            error = True
            problems.append(line)
    msg = "\n".join(problems)
except Exception as e:
    print >> sys.stderr, e
    error = True
    msg = "get %s failed" % uri

if error:
    if VERBOSE:
        print >> sys.stderr, msg
    sys.exit(1)
else:
    sys.exit(0)