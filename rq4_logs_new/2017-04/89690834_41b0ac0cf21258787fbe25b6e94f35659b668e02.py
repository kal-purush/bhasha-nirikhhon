#!/usr/bin/env python

import pexpect
import textfsm
import yaml
import sys
import os.path
import multiprocessing
from sys import argv

if len(argv)<5:
    print """
    Usage: {} device_dictionary.yaml command template output.yaml
        - device_dictionary - dict file in yaml format
        - command - e.q 'show vlan'
        - template - file or None
        - output - file to result in yaml format
    """.format(argv[0])
    sys.exit()

# File with login and pass to telnet connect to devices
req = yaml.load(open("req.yaml"))

dev_dict, command, template, output = argv[1:]

if template == "None":
    template = None
elif os.path.isfile(template) == False:
    print "Template file {} does not exists!".format(template)
    sys.exit()

dic = yaml.load(open(dev_dict))

def search (input,template):
    tmpl = textfsm.TextFSM(open(template))
    return tmpl.ParseText(input)

def conn (device,command,queue,template=None):
    """
    Connect to host by telnet.
    Connection timeout 5 seconds
    """
    try:
        t = pexpect.spawn('telnet {}'.format(device))
        t.expect('User Name:', 5)
        t.sendline(req['user'])
        t.expect('Password:')
        t.sendline(req['password'])
        t.expect('#')
        t.sendline('terminal datadump')
        t.expect('#')
        t.sendline(command)
        t.expect('#')
        if template != None:
            queue.put({device: search(t.before,template)})
        else:
            queue.put({device: t.before})
    except pexpect.TIMEOUT:
        queue.put(None)

def do_proccesses (function,items,command,template=None):
    """ Func for use multiproces """
    results = []
    processes = []
    queue = multiprocessing.Queue()

    for i,it in enumerate(items):
        #if i == 10: break
        p = multiprocessing.Process(target = function, args = (it, command, queue, template))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    for p in processes:
        results.append(queue.get())

    return results

# dic must contain the key 'switches'
# if not, change it below
with open(output, "w") as v:
    v.write(
        yaml.dump(do_proccesses(
            conn,
            dic['switches'],
            command,
            template)
        )
    )