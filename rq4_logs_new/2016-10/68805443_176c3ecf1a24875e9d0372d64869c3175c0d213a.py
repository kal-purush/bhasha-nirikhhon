#-*- coding: UTF-8 -*-
import socket,select
import sys
import threading
from multiprocessing import Process
class Proxy:
    def __init__(self,soc):
        self.client,_ = soc.accept()
        self.target = None
        self.request_url = None
        self.BUFSIZE = 1024
        self.method = None
        self.targetHost = None

    def getClientRequest(self):
        request = self.client.recv(self.BUFSIZE).decode()
        if not request:
            return None
        cn = request.find('\n')
        firstLine = request[:cn]
        print(firstLine[:len(firstLine) - 9])
        line = firstLine.split()
        self.method = line[0]
        self.targetHost = line[1]
        return request

    def  getHeader(self,request):
        cn = request.find('\n')
        done = request[cn + 1:]
        #print(done)
        headers = {}
        #print('!!!!!!!!!!!!!!!')
        for i in done.split('\r\n\r\n')[0].split('\r\n'):
            #print(i)

            if i != '':
                k, v = i.split(': ')
                headers[k] = v
        #print(headers)
        data = {}
        for i in done.split('\r\n\r\n')[1].split('&'):
            if i != '':
                k,v = i.split('=')
                data[k] = v
        #print(data)
        return [headers,data]


    def commonMethod(self,request):
        tmp = self.targetHost.split('/')
        net = tmp[0]+'//'+ tmp[2]
        request = request.replace(net,'')
        targetAddr = self.getTargetInfo(tmp[2])
        #print(targetAddr)
        '''
        try:
            (fam,_,_,_,addr) = socket.getaddrinfo(targetAddr[0],targetAddr[1])[0]
        except Exception as e:
            print(e)
            return
        '''
        #self.target=socket.socket(fam)
        #self.target.connect(addr)
        self.target = socket.socket()
        #self.target.setblocking(False)
        self.target.connect(targetAddr)
        self.target.send(request.encode())
        self.nonblocking()

    def connectMethod(self,request): #对于CONNECT处理可以添加在这里
        pass
    def run(self):
        request = self.getClientRequest()
        if request:
            self.getHeader(request)
        if request:
            if self.method in ['GET','POST','PUT',"DELETE",'HAVE']:
                self.commonMethod(request)
            elif self.method == 'CONNECT':
                self.connectMethod(request)

    def nonblocking(self):
        inputs = [self.client,self.target]
        while True:
            readable,writeable,errs = select.select(inputs,[],inputs,3)
            if errs:
                break
            for soc in readable:
                data = soc.recv(self.BUFSIZE)
                #print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
                #print(data)
                if data:
                    if soc is self.client:
                        self.target.send(data)
                        print(data)
                    elif soc is self.target:
                        self.client.send(data)
                else:
                    break
        self.client.close()
        self.target.close()

    def getTargetInfo(self,host):
        port = 0
        site = None
        if ':' in host:
            tmp = host.split(':')
            site = tmp[0]
            port = int(tmp[1])
        else:
            site = host
            port = 80
        return site,port

if __name__ == '__main__':
    host = '0.0.0.0'
    port = 8088
    backlog = 5
    server = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    server.bind((host,port))
    server.listen(50)
    while True:
        t = threading.Thread(target=Proxy(server).run)
        t.start()