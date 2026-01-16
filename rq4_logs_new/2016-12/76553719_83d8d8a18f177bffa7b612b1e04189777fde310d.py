# coding:utf8
import struct
import socket
import time

BC_PORT = 10000

CT_NONE = 0
CT_H1 = 1
CT_H1N = 2
CT_M6S = 3
CT_AIR = 4
CT_M2PRO = 5
CT_M6TT = 6

D = {0:'NONE',
     1:'H1',
     2:'H1N',
     3:'M6S',
     4:'AIR',
     5:'M2PRO',
     6:'M6TT'
}
R = {0:"Unregistered",
     1:"Registered"
}

class searchCamera(object):
    """
    typedef struct searchBrodcastHeader
{
    char            protocolHeader[4];   //协议头  4+2+1+8+4+4=
    short           controlMask;         //操作码
    char            reserved;            //保留
    char            reserved2[8];        //保留
    int             contentLength;       //正文长度
    int             reserved3;           //保留

}IB_searchBrodcastHeader;

4shc8sii

typedef struct searchCommandContentReply
{
    IB_searchBrodcastHeader  header; //头部
    char            camID[13];          //摄像头ID  4shc8sii13s21sIIIIc3s4s4sHc  13+21+4+4+4+4+1+3+4+4+2+1=65
    char            camName[21];        //摄像头名称  4+2+1+8+4+4+13+21+4+4+4+4+1+3+4+4+2+1 =
    unsigned int    ip;                 //IP; 大端
    unsigned int    netMask;            //掩码  大端
    unsigned int    getwayIP;           //网关IP;  大端
    unsigned int    DNS;                //DNS;  大端
    char			p2pType;			// 0:TUTK 1:SY P2P
    char            reserved[3];        //保留
    char            sysVersion[4];      //系统版本   固件版本 = sysVersion[0].sysVersion[1].sysVersion[2]
    char            appVersion[4];       //App software 版本  appVersion[0]摄像头类型
    unsigned short  port;               //端口 大端
    char            dhcpEnabled;        //DHCP状态
}IB_searchCommandContentReply;
    """
    def __init__(self):
        self.protocolHeader = ' '
        self.controlMask = 0
        self.reserved = ' '
        self.reserved2 = ' '
        self.contentLength = 0
        self.reserved3 = 0

        #     将要广播的字符串（broadcastString），转化成网络传输的字节码
        self.broadcastString = struct.pack('4shc8sii', self.protocolHeader, self.controlMask,
                                           self.reserved, self.reserved2, self.contentLength,
                                           self.reserved3)

    #广播字节码并接收回应
    def getIp(self, values):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # 重复发送5次广播包，每次间隔0.1秒，接收时不停的循环接收，提高搜索成功率
        for i in xrange(7):  # @UnusedVariable
            s.sendto(values, ('<broadcast>', BC_PORT))  # BC_PORT=1000是端口号，要和固件方对应
            time.sleep(0.1)
        s.setblocking(0)  # 设置为非阻塞模式
        # 定义一个空列表，用来接收camID和IP
        l = []
        while True:
            try:
                # print "hehe"
                sock, addr = s.recvfrom(1024)
                # print "sock=",sock
                # print "len=",len(sock)

                reply = struct.unpack('<4shc8sii13s21sIIIIc3s4s4sHc', sock)
                # print "reply=",reply
                # print "camid = ",reply[6]
                # print "uid = ",reply[7]
                ip = socket.inet_ntoa(struct.pack('I', socket.htonl(reply[8])))
                # print "ip = ",ip
                # print "p2pType = ",reply[12]
                l.append(reply)
                # print l
                #列表去重，因为有多次搜索，可能会有很多重复的返回值
                j = list(set(l))
                # print j
            except socket.error:
                break
        try:
            # print j
            return j
        except:
            return 0

if __name__ == "__main__":
    sc = searchCamera()
    lis = sc.getIp(sc.broadcastString)
    for i in lis:
        print i