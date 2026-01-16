#!/usr/bin/python
import os
import json
import platform
from subprocess import call, Popen, PIPE
from termcolor import colored
import socket
def run(command):
	process=Popen(
		args=command,
		stdout=PIPE,
		shell=True
	)
	return process.communicate()[0].strip('\n')
sysinfo={} 
sysinfo['Date']=run('date')
sysinfo['Uptime']=run('uptime')
sysinfo['Arch']=platform.machine()
uname=platform.uname()
sysinfo['System']=uname[0]
sysinfo['0-Hostname']=uname[1]
sysinfo['Release']=uname[2]
sysinfo['IP Address']=socket.gethostbyname(socket.gethostname())
systeminfo=json.dumps(sysinfo, sort_keys=True, indent=4, separators=(',', ': '))
print ("\n"+ colored('System Information:','cyan')+"\n")
for key,value in sorted(sysinfo.items()):
	print ("\t"+colored(key.strip("0-"),'green') +": "+colored(value,'yellow'))
print ("\n")
file='serverinfo.json'
of=open(file,'wb')
of.write(systeminfo)
of.close
print ("System Information is written to "+file)