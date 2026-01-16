#!/bin/env python
# -*- coding: utf_8 -*-
import sys
import yaml
import time
import os
import commands
import socket

reload(sys)
sys.setdefaultencoding('utf8')

timeout = 60

remoteCpuInfoPy = "/data/sh/cpuInfo.py"

remoteGameBase = "/data/ywygame_dtl"

yamlFile = "/root/sh/config.yaml"

validPortRangelist = (30011, 30021, 30031, 30041, 30051, 30061, 30071, 30081, 30091, 30111, 30211, 30311, 30411, 30511, 30611, 30711, 30811, 30911)

yamlFileDescriptor = open(yamlFile)

yamlContent = yaml.load(yamlFileDescriptor)

yamlFileDescriptor.close()

def IsOpen(ip,port):
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    try:
        s.connect((ip,int(port)))
        s.shutdown(2)
        return True
    except:
        return False

def validPortNumber(ip, validPortRangelist):
    for i in validPortRangelist:
        if not IsOpen(ip, i):
           return i

def GetServerIP(yamlContent, yamlkey, serName):
    for i in yamlContent[yamlkey]:
        if i.has_key(serName):
            return i[serName]
        
def printServerList(yamlContent, yamlkey, remoteCpuInfoPy):
    '''打印配置文件中的服务器列表'''
    for i in yamlContent[yamlkey]:
        for key, value in i.items():
            print "服务器简写%s, 实际IP地址为%s, CPU使用率为:%d%%" % (key, value, round(serverCpuInfo(value, remoteCpuInfoPy)))

def serverCpuInfo(serverIP, remoteCpuInfoPy):
    try:
        return float(commands.getoutput("ssh -p65521 root@{0} {1}".format(serverIP, remoteCpuInfoPy)))
    except Excetion as e:
        print "获取远程CPU使用率发生错误，检查服务器连通情况"
        exit(1)

def isValidDate(dateStr):
    '''判断是否是一个有效的日期字符串'''
    try:
        time.strptime(dateStr, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False

def validServName(serName):
    for i in yamlContent["servername"]:
        if i.has_key(serName):
            return True

    return False    

def validServId(serId):
    try:
        serId = int(serId)
        return True
    except ValueError:
        print "关键参数错误，配置文件里的服务器ID不是数字，请重新配置，程序退出"
        exit(1)

def inputServerInfo(yamlContent, yamlkey):
    confirmInfo = True
    while confirmInfo:
        print "============================================="
        printServerList(yamlContent, yamlkey, remoteCpuInfoPy)
        print "============================================="
        serName = raw_input("根据以上信息选择你要把游戏服务器安装到哪一个物理服务器上(输入简写),不建议安装到CPU使用率上85%的服务器上:")
##       serId = raw_input("请输入服务器id:")
        if validServName(serName):## and validServId(serId):
            confirmInfo = False
        else:
            print "输入服务器简写名字不正确，请重新输入"

    serverIP =  GetServerIP(yamlContent, yamlkey, serName)
    
    portNumber = validPortNumber(serverIP, validPortRangelist)

    return serName , serverIP, int(portNumber)


if __name__=="__main__": 
    serName, serverIP, portNumber = inputServerInfo(yamlContent, "servername")    
    print "你选择的物理服务器是 %s" % (serName)
    
    serId = yamlContent["serverId"]

    if validServId(serId):
        serId = int(serId)
        print "你的游戏服务器ID是 %d" % (serId)

    opendate = yamlContent["opendate"]

    if isValidDate(opendate): 
        print "你配置的时间是%s" % (opendate)

    zonename = yamlContent["zonename"]
    print "你配置的大区名称是%s" % (zonename)
   
    print "你选择的服务器IP%s" % (serverIP)

    print "你选择的服务器端口%d" % (portNumber)
 
    confirm = True
    while confirm:
        go = raw_input("确认以上信息(y/n):")
        if go == "Y" or go == "y":
            break
        else:
            print "信息不正确退出"
            exit(1)
    
    gameSeverName = "gameserver_" + str(serId)

    remoteSHScript = os.path.join(remoteGameBase, gameSeverName, "script", "start_gs.sh")

    remoteGamePath =  os.path.join(remoteGameBase, gameSeverName)

    print  remoteSHScript
    ausibleCommandEbin = "ansible %s -m copy -a \"src=/root/game/game_dir/ dest=/data/install_game_dtl owner=ywygame group=ywygame mode='u=rw,g=r,o=r'\" " % (serName)
    ausibleCommandInstall = "ansible %s -a \"/data/sh/install_game_dtl.py %d '%s' %s %d\"" % (serName, serId, opendate, zonename, portNumber) 
    ausibleCommandStartGame = "ansible %s -a \"%s\"" % (serName, remoteSHScript)   
    ausibleCommandOwner = "ansible %s -m file -a \"path=%s owner=ywygame group=ywygame recurse=yes\"" % (serName, remoteGamePath)
    ansibleRMcommand = "ansible LOGIN -m command -a \"/data/sh/rmCacheFile.py\""


     
    print "执行远程安装脚本 " 
    os.system(ausibleCommandInstall)    
    
    print "修改服务器文件夹权限"
    os.system(ausibleCommandOwner)

    print "删除临时文件"
    print commands.getoutput(ansibleRMcommand)

    print "开始启动服务器"
    os.system(ausibleCommandStartGame)
    startTime = time.time()
     
    count = 0
    while True:
        endTime = time.time()
        timeInterval = endTime - startTime
        count += 1
        sys.stdout.write("-" * count + "\r")
        sys.stdout.flush()
        if timeInterval < timeout:
            if IsOpen(serverIP, portNumber):
                print "游戏服务器id为%d成功安装在IP地址为%s上，服务器端口为%d, 并服务器启动完成" % (serId, serverIP, portNumber)
                break
        elif timeInterval >= timeout:
            print "启动服务器超时，请登录到%s查看原因" % (serverIP)
            break
        time.sleep(1)

#    print "拷贝程序文件到物理服务器%s" % (serName)
#    os.system(ausibleCommandEbin)
