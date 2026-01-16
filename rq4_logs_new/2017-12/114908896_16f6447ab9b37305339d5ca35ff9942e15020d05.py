import re
import csv
import sqlite3
import matplotlib
import matplotlib.pyplot as plt
#matplotlib.use('Agg')
from pylab import *
from matplotlib.pyplot import plot,savefig

#myfont = FontProperties(fname='/usr/share/fonts/truetype/wqy/wqy-microhei.ttc', size=20)
#rcParams['axes.unicode_minus'] = False  # 解决负号'-'显示为方块的问题

p = re.compile(
    r'^(?P<timestamp>[\d:.]+) IP \(.*, id (?P<id>\d+), offset (?P<offset>\d+), flags \[(?P<ip_flags>.*)\], proto (?P<proto>\w+) .*?, length (?P<ip_length>\d+).*\)$'
    r'(\s*^\s*(?P<src_ip>\d+.\d+.\d+.\d+).(?P<src_port>\d+) > (?P<dst_ip>\d+.\d+.\d+.\d+).(?P<dst_port>\d+):( Flags \[(?P<tcp_flags>[^,]+)\])?(, cksum [^,]+)?(, seq (?P<seqno>\d+(:\d+)?))?(, ack (?P<ackno>\d+))?.*$)?',
    re.M)
#p1 = re.compile(r'^(?P<timestamp>[\d:.]+) IP6 \(.*, next-header (?P<proto>\w+) .*? payload length: (?P<ip_length>\d+)\)'
 #               r'(\s*^(?P<src_ip>[\da-f:]+).(?P<src_port>\d+) > (?P<dst_ip>[\da-f:]+).(?P<dst_port>\d+):( Flags \[(?P<tcp_flags>[UAPRSF.|none]+)\])?.*$)?',
#                re.M)

# tcpdump -nnv ip >> tcpdump.out
with open('tcpdump1.out') as f:
    s = f.read()
    s = list(p.finditer(s))

with open('tcpdump1.out.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, p.groupindex.keys())
    writer.writeheader()
    for m in s:
        writer.writerow(m.groupdict())

with sqlite3.connect(':memory:') as con:
    con.execute(f'''CREATE TABLE tcpdumpout
                    ({','.join([x+' TEXT NULL' for x in p.groupindex.keys()])})
                ''')
    for m in s:
        con.execute(f'''INSERT INTO tcpdumpout VALUES
                        ({','.join(['"'+x+'"' if x else 'NULL' for x in m.groupdict().values()])})
                    ''')

    # IP分组携带不同协议的分组数，饼状图
    res1_packet = con.execute(f'''SELECT proto, COUNT(proto)
                                  FROM tcpdumpout
                                  WHERE src_ip=="183.172.104.52"
                                  GROUP BY proto
                              ''').fetchall()
    fig1 = plt.figure(1)
    labels = []
    sizes = []
    for i in range(len(res1_packet)):
        labels.append(res1_packet[i][0])
        sizes.append(res1_packet[i][1])
    plt.axes(aspect='equal')
    plt.title('IP protocal(packets)')
    plt.pie(sizes, explode=[0 for x in range(len(res1_packet))], labels=labels, autopct='%1.1f%%', shadow=False, startangle=90)
    plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/IPprotocal(packets)')
    plt.close()
    #plt.show()
    # IP分组携带不同协议的总数据量
    res1_len = con.execute(f'''SELECT proto, SUM(ip_length)
                              FROM tcpdumpout
                              WHERE src_ip=="183.172.104.52"
                              GROUP BY proto
                           ''').fetchall()
    fig2 = plt.figure(2)
    labels = []
    sizes = []
    for i in range(len(res1_len)):
        labels.append(res1_len[i][0])
        sizes.append(res1_len[i][1])
    plt.axes(aspect='equal')
    plt.title('IP protocal(quantity of data)')
    plt.pie(sizes, explode=[0 for x in range(len(res1_len))], labels=labels, autopct='%1.1f%%', shadow=False, startangle=90)
    plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/IP protocal(quantity of data)')
    plt.close()
    #plt.show()

    # 多少IP分组是片段
    res2_frag = con.execute(r'''SELECT COUNT(*)
                                FROM tcpdumpout
                                WHERE ip_flags=="MF" AND src_ip=="183.172.104.52"
                            ''').fetchall()
    # 多少IP数据报被分片

    res2_packet = con.execute(r'''SELECT COUNT(DISTINCT id)
                                  FROM tcpdumpout
                                  WHERE ip_flags=="MF" AND src_ip=="183.172.104.52"
                              ''').fetchall()
    # 载荷为TCP有多少比例的IP数据报被分片
    res2_packet_tcp = con.execute(r'''SELECT COUNT(DISTINCT id)
                                      FROM tcpdumpout
                                      WHERE ip_flags=="+" AND proto=="TCP" AND src_ip=="183.172.104.52"
                                  ''').fetchall()
    # 载荷为UDP有多少比例的IP数据报被分片
    res2_packet_udp = con.execute(r'''SELECT COUNT(DISTINCT id)
                                      FROM tcpdumpout
                                      WHERE ip_flags=="+" AND proto=="UDP" AND src_ip=="183.172.104.52"
                                  ''').fetchall()

    # IP数据报长度的累积分布
    res3_ip = con.execute(r'''SELECT SUM(ip_length) 
                              FROM tcpdumpout
                              WHERE src_ip=="183.172.104.52"
                              GROUP BY id
                              ORDER BY SUM(ip_length)
                          ''').fetchall()
    a = {}
    b = []
    for i in range(1500):
        t = (i,)
        a[i] = res3_ip.count(t)
    for i in range(1, 1500):
        a[i] = a[i] + a[i - 1]
        b.append(a[i])
    c = range(1, 1500)
    fig=plt.figure();
    plt.plot(c, b, linewidth=1.0)
    plt.xlabel('packet length')
    plt.ylabel('number')
    plt.title('ip packet cumulative distribution')
    plt.grid(True)
    plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/ip packet cumulative distribution')
    plt.close()
    #show()
    # TCP数据报长度的累积分布
    res3_tcp = con.execute(r'''SELECT SUM(ip_length)
                               FROM tcpdumpout
                               WHERE proto=="TCP" AND src_ip=="183.172.104.52"
                               GROUP BY id
                               ORDER BY SUM(ip_length)
                           ''').fetchall()
    a = {}
    b = []
    for i in range(1500):
        t = (i,)
        a[i] = res3_tcp.count(t)
    for i in range(1, 1500):
        a[i] = a[i] + a[i - 1]
        b.append(a[i])
    c = range(1, 1500)
    fig=plt.figure()
    plt.plot(c, b, linewidth=1.0)
    plt.xlabel('packet length')
    plt.ylabel('number')
    plt.title('tcp packet cumulative distribution')
    plt.grid(True)
    plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/tcp packet cumulative distribution')
    plt.close()
    #show()
    # UDP数据报长度的累积分布
    res3_udp = con.execute(r'''SELECT SUM(ip_length)
                               FROM tcpdumpout
                               WHERE proto=="UDP" AND src_ip=="183.172.104.52"
                               GROUP BY id
                               ORDER BY SUM(ip_length)
                           ''').fetchall()
    a = {}
    b = []
    for i in range(1500):
        t = (i,)
        a[i] = res3_udp.count(t)
    for i in range(1, 1500):
        a[i] = a[i] + a[i - 1]
        b.append(a[i])
    c = range(1, 1500)
    fig=plt.figure()
    plt.plot(c, b, linewidth=1.0)
    plt.xlabel('packet length')
    plt.ylabel('number')
    plt.title('udp packet cumulative distribution')
    plt.grid(True)
    plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/udp packet cumulative distribution')
    plt.close()
    #show()

    # tcp端口分布直方图
    res4_tcp = con.execute(r'''SELECT src_port
                                  FROM tcpdumpout
                                  WHERE proto=="TCP" AND src_ip=="183.172.104.52"
                                  ''').fetchall()
    fig = plt.figure(3)
    plt.hist(res4_tcp, 700)  # cumulative=True
    plt.xlabel('port')
    plt.ylabel('packet number')
    plt.title('tcp port packer number')
    plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/tcp port packer number')
    plt.close()
    #plt.show()
    # udp端口分布直方图
    res4_udp = con.execute(r'''SELECT src_port
                                      FROM tcpdumpout
                                      WHERE proto=="UDP" AND src_ip=="183.172.104.52"
                                      ''').fetchall()
    fig = plt.figure(4)
    plt.hist(res4_udp, 70)  # cumulative=True
    plt.xlabel('port')
    plt.ylabel('packet number')
    plt.title('udp port packer number')
    plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/udp port packer number')
    plt.close()
    #plt.show()
    # tcp前10端口数据报长度的累积分布 源端口
    res5_tcp = con.execute(r'''SELECT src_port, COUNT(src_port)
                                      FROM tcpdumpout
                                      WHERE proto=="TCP" AND src_ip=="183.172.104.52"
                                      GROUP BY src_port 
                                      ORDER BY COUNT(src_port) DESC
                                          ''').fetchall()
    res5_tcp = res5_tcp[0:10]
    for i in res5_tcp:
        res5_tcp_port = con.execute(r'''SELECT SUM(ip_length) 
                                       FROM tcpdumpout
                                       WHERE proto=="TCP" AND src_port==''' + str(i[0]) + ''' AND src_ip=="183.172.104.52"
                                       GROUP BY id
                                       ORDER BY SUM(ip_length)
                                   ''').fetchall()
        a = {}
        b = []
        for j in range(1500):
            t = (j,)
            a[j] = res5_tcp_port.count(t)
        for j in range(1, 1500):
            a[j] = a[j] + a[j - 1]
            b.append(a[j])
        c = range(1, 1500)
        fig=plt.figure()
        plt.plot(c, b, linewidth=1.0)
        plt.xlabel('packet length')
        plt.ylabel('number')
        plt.title('tcp port ' + str(i[0]) + ' packet cumulative distribution')
        plt.grid(True)
        plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/tcp_srcport/['+str(i[1])+']tcp port ' + str(i[0]) + ' packet cumulative distribution ')
        plt.close()
        #show()
    # tcp前10端口数据报长度的累积分布 目的端口
    res5_tcp = con.execute(r'''SELECT dst_port, COUNT(dst_port)
                                          FROM tcpdumpout
                                          WHERE proto=="TCP" AND src_ip=="183.172.104.52"
                                          GROUP BY dst_port 
                                          ORDER BY COUNT(dst_port) DESC
                                              ''').fetchall()
    res5_tcp = res5_tcp[0:10]
    for i in res5_tcp:
        res5_tcp_port = con.execute(r'''SELECT SUM(ip_length) 
                                           FROM tcpdumpout
                                           WHERE proto=="TCP" AND dst_port==''' + str(i[0]) + ''' AND src_ip=="183.172.104.52"
                                           GROUP BY id
                                           ORDER BY SUM(ip_length)
                                       ''').fetchall()
        a = {}
        b = []
        for j in range(1500):
            t = (j,)
            a[j] = res5_tcp_port.count(t)
        for j in range(1, 1500):
            a[j] = a[j] + a[j - 1]
            b.append(a[j])
        c = range(1, 1500)
        fig = plt.figure()
        plt.plot(c, b, linewidth=1.0)
        plt.xlabel('packet length')
        plt.ylabel('number')
        plt.title('tcp port ' + str(i[0]) + ' packet cumulative distribution')
        plt.grid(True)
        plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/tcp_dstport/[' + str(i[1]) + ']tcp port ' + str(
            i[0]) + ' packet cumulative distribution ')
        plt.close()
        # show()

    # udp 前10名端口数据报长度的累积分布  源端口
    res5_udp = con.execute(r'''SELECT src_port, COUNT(src_port)
                                  FROM tcpdumpout
                                  WHERE proto=="UDP"  AND src_ip=="183.172.104.52"
                                  GROUP BY src_port 
                                  ORDER BY COUNT(src_port) DESC
                                      ''').fetchall()
    res5_udp = res5_udp[0:10]
    for i in res5_udp:
        res5_udp_port = con.execute(r'''SELECT SUM(ip_length) 
                                               FROM tcpdumpout
                                               WHERE proto=="UDP" AND src_port==''' + str(i[0]) + ''' AND src_ip=="183.172.104.52"
                                               GROUP BY id
                                               ORDER BY SUM(ip_length)
                                           ''').fetchall()
        a = {}
        b = []
        for j in range(1500):
            t = (j,)
            a[j] = res5_udp_port.count(t)
        for j in range(1, 1500):
            a[j] = a[j] + a[j - 1]
            b.append(a[j])
        c = range(1, 1500)
        fig=plt.figure()
        plt.plot(c, b, linewidth=1.0)
        plt.xlabel('packet length')
        plt.ylabel('number')
        plt.title('udp port ' + str(i[0]) + ' packet cumulative distribution')
        plt.grid(True)
        plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/udp_srcport/[' + str(i[1]) + ']udp port ' + str(
            i[0]) + ' packet cumulative distribution ')
        plt.close()
        #show()
    # udp 前10名端口数据报长度的累积分布  目的端口
    res5_udp = con.execute(r'''SELECT dst_port, COUNT(dst_port)
                                      FROM tcpdumpout
                                      WHERE proto=="UDP"  AND src_ip=="183.172.104.52"
                                      GROUP BY dst_port 
                                      ORDER BY COUNT(dst_port) DESC
                                          ''').fetchall()
    res5_udp = res5_udp[0:10]
    for i in res5_udp:
        res5_udp_port = con.execute(r'''SELECT SUM(ip_length) 
                                               FROM tcpdumpout
                                               WHERE proto=="UDP" AND dst_port==''' + str(i[0]) + ''' AND src_ip=="183.172.104.52"
                                               GROUP BY id
                                               ORDER BY SUM(ip_length)
                                           ''').fetchall()
        a = {}
        b = []
        for j in range(1500):
            t = (j,)
            a[j] = res5_udp_port.count(t)
        for j in range(1, 1500):
            a[j] = a[j] + a[j - 1]
            b.append(a[j])
        c = range(1, 1500)
        fig=plt.figure()
        plt.plot(c, b, linewidth=1.0)
        plt.xlabel('packet length')
        plt.ylabel('number')
        plt.title('udp port ' + str(i[0]) + ' packet cumulative distribution')
        plt.grid(True)
        plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/udp_dstport/[' + str(i[1]) + ']udp port ' + str(
            i[0]) + ' packet cumulative distribution ')
        plt.close()
        #show()
    # tcp控制位出现百分比
    tcp_f = con.execute(r'''SELECT tcp_flags ,COUNT(tcp_flags)
                                  FROM tcpdumpout
                                  WHERE proto=="TCP" AND src_ip=="183.172.104.52"
                                  GROUP BY tcp_flags
                                  ''').fetchall()
    a = {'C': 0, 'E': 0, 'A': 0, 'P': 0, 'R': 0, 'S': 0, 'F': 0}
    del tcp_f[0]
    for i in tcp_f:
        if 'C' in i[0]:
            a['C'] = a['C'] + i[1]
        if 'E' in i[0]:
            a['E'] = a['E'] + i[1]
        if 'U' in i[0]:
            a['U'] = a['U'] + i[1]
        if 'A' in i[0]:
            a['A'] = a['A'] + i[1]
        if 'P' in i[0]:
            a['P'] = a['P'] + i[1]
        if 'R' in i[0]:
            a['R'] = a['R'] + i[1]
        if 'S' in i[0]:
            a['S'] = a['S'] + i[1]
        if 'F' in i[0]:
            a['F'] = a['F'] + i[1]
    fig5 = plt.figure(5)
    labels = list(a.keys())
    sizes = list(a.values())
    plt.axes(aspect='equal')
    plt.title('tcp control (quantity of data)')
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=False, startangle=90)
    plt.savefig('/home/zyy/桌面/computer network/photo/liuchu/tcp control (quantity of data)')
    plt.close()
    #plt.show()
















