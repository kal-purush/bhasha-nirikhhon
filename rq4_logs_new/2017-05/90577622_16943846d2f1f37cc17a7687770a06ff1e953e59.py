import socket,select,threading,sys;

host=socket.gethostname()

addr=(host,5963)

def conn():
    s=socket.socket()
    s.connect(addr)
    return s

def lis(s):
    my=[s]
    while True:
        r,w,e=select.select(my,[],[])
        if s in r:
            try:
                print s.recv(1024)
            except socket.error:
                print 'socket is error'
                exit()

def talk(s):
    while True:
        try:
            info=raw_input()
        except Exception,e:
            print 'can\'t input'
            exit()
        try:
            s.send(info)
        except Exception,e:
            print e
            exit()

def main():
    ss=conn()
    t=threading.Thread(target=lis,args=(ss,))
    t.start()
    t1=threading.Thread(target=talk,args=(ss,))
    t1.start()
if __name__=='__main__':
    main()