#!/usr/bin/env python
# -*- coding:utf-8 -*-

import socket, time,threading
socket.setdefaulttimeout(3) #set the time of timeout

def socket_port(ip, port):
    try:
        if port >=65535:
            print('scan ports ended')
        s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result=s.connect_ex((ip, port))
        if result==0:
            lock.acquire()
            print (ip,':',port,'has been used')
            lock.release()
    except Exception  as e:
        print(1)
        print(e)
        print ('exception throws when scanning ports')

def ip_scan(ip):
    try:
        print ('begin scanning ip %s' % ip)
        start_time=time.time()
        for i in range(3305,3309):
            threading.Thread(target=socket_port(ip, int(i)))
        print ('scan port ended，used：%.2f' %(time.time()-start_time))
    except  Exception as e:
        print(2)
        print (e)
        print ('exception throws when scanning ports')

if __name__=='__main__':
    url=input('Input the ip you want to scan: ')
    lock=threading.Lock()
    ip_scan(url)      