# -*- coding: utf-8 -*-

import json
import time
import gevent
import urllib2


def fetch(pid):
    response = urllib2.urlopen('http://test.btcjp.me/time')
    result = json.loads(response.read())
    print('Process %s: %s' % (pid, result['now']))


def sync():
    start = time.time()
    for i in range(10):
        fetch(i)
    time_cost = time.time() - start
    print('sync time cost: %.5f' % time_cost)


def async():
    start = time.time()
    threads = []
    for i in range(10):
        threads.append(gevent.spawn(fetch, i))
    gevent.joinall(threads)
    time_cost = time.time() - start
    print('async time cost: %.5f' % time_cost)


print('Sync:')
sync()

print('Async:')
async()


'''
Sync:
    Process 0: 1464754877.25
    Process 1: 1464754877.47
    Process 2: 1464754877.7
    Process 3: 1464754877.92
    Process 4: 1464754878.15
    Process 5: 1464754878.36
    Process 6: 1464754878.59
    Process 7: 1464754878.82
    Process 8: 1464754879.02
    Process 9: 1464754879.23
    sync time cost: 2.59578
Async:
    Process 0: 1464754879.43
    Process 1: 1464754879.64
    Process 2: 1464754879.85
    Process 3: 1464754880.05
    Process 4: 1464754880.25
    Process 5: 1464754880.47
    Process 6: 1464754880.67
    Process 7: 1464754880.87
    Process 8: 1464754881.09
    Process 9: 1464754881.3
    async time cost: 2.07313
'''