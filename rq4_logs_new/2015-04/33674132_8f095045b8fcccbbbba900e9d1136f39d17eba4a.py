#!/usr/bin/python

import sys
import os
import subprocess
import stat

def insert_module():
    print '****** Insert Module ******\n'

    print '1: insmod chdev.ko'
    subprocess.call(['sudo', 'insmod', 'chdev.ko'])
    print 'success\n'

    ps = subprocess.Popen(('dmesg'), stdout=subprocess.PIPE)
    output = subprocess.check_output(('tail', '-n 5'), stdin=ps.stdout)
    ps.wait()

    numbers = [ int(n) for n in output.split() if n.isdigit()]

    major = numbers[1];
    print 'MAJOR NUMBER:', major, '\n'

    print '2: mknode'
    filename = "/dev/mychdev"
    mode = 0777|stat.S_IFCHR
    dev = os.makedev(major, 0)

    os.mknod(filename, mode, dev)
    print 'success'

def remove_module():
    print '1: remove chdev.ko'
    subprocess.call(['sudo', 'rmmod', 'chdev.ko'])
    print 'success\n'

    print '1: remove chdev.ko'
    subprocess.call(['sudo', 'rm', '/dev/mychdev'])
    print 'success\n'



# ---------------------------- Main --------------------------------------

functions = {
    'in': insert_module,
    'rm': remove_module,    
}

try:
    if 1 < len (sys.argv):
        func = functions[sys.argv[1]]
        func()
    else:
        print 'Wrong option or option has not been specified'
except KeyError:
    print 'Wrong option or option has not been specified'


# ------------------------------------------------------------------------



