#!/usr/bin/env python
import sys
import os
from subprocess import Popen, PIPE

class Sync(object):
    def __init__(self):
        self.rp, self.wp = os.pipe()

    def notify(self):
        os.write(self.wp, '1')

    def wait(self):
        os.read(self.rp, 1)

    def close(self):
        for fd in (self.rp, self.wp):
            os.close(fd)


def do_test(umask):
    print 'Testing with umask 0{0:03o}'.format(umask)

    umask_set = Sync()
    finished = Sync()

    pid = os.fork()
    if pid == 0:
        ### child
        # set umask
        os.umask(umask)

        # tell parent we've set umask
        #print 'child: notifying parent'
        umask_set.notify()

        #print 'child: waiting to die'
        finished.wait()

        #print 'child: finished'

        sys.exit(0)

    ### parent
    # Wait for child to set umask
    umask_set.wait()

    # Invoke 'getumask'
    getumask = Popen(['./getumask', str(pid)], stdout=PIPE, stderr=PIPE)
    out, err = getumask.communicate()

    # Tell child to die
    finished.notify()

    umask_set.close()
    finished.close()

    assert(getumask.returncode == 0)
    result = int(out.strip())

    assert(result == umask)

if __name__ == '__main__':
    do_test(0)
    do_test(0345)