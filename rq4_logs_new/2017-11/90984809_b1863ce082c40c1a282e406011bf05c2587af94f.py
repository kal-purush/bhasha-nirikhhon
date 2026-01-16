#!/usr/bin/python3
#-*- coding:utf-8 -*-

import socket
import sys


Route_1 = {
	'name': 'Route_1',
	'ip': '127.0.10.1',
	'port': 55550,
}

Route_2 = {
	'name': 'Route_2',
	'ip': '127.0.20.1',
	'port': 55560,
}
Route_3 = {
	'name': 'Route_3',
	'ip': '127.0.30.1',
	'port': 55570,
}

Host_1 = {
	'name': 'host1',
	'ip': '127.0.10.2',
	'port': 55010,
	'nextRoute': Route_1	
}
Host_2 = {
	'name': 'host2',
	'ip': '127.0.20.2',
	'port':  55020,
	'nextRoute': Route_2
}
Host_3 = {
	'name': 'host3',
	'ip': '127.0.30.2',
	'port': 55030,
	'nextRoute': Route_3
}


def send(curr_host,nextIp,port,data):
	client = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	try:
		client.sendto(data.encode('utf-8'),(nextIp,port))
		response = client.recv(4096).decode('utf-8')
		if response == 'ACK':
			print("[%s] send to route :  %s : %d success!" % (curr_host['name'],nextIp,port))
	except Exception as err:
		print('[%s] Send data Error: %s '%(curr_host['name'],err))

	client.close()

def recv(curr_host):
	server = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	server.bind((curr_host['ip'],curr_host['port']))

	# 接收数据
	# while True:
	print('Wait message ....')
	reMsg = 'ACK'
	data,addr = server.recvfrom(4096)
	server.sendto(reMsg.encode('utf-8'),addr)
	data = data.decode('utf-8').split('|')
	print("[%s] Accepted from host %s:%d " %(curr_host['name'],data[2],int(data[3])))
	print("[%s] Msg is : %s "%(curr_host['name'],data[4]))

	server.close()


def main(curr_host):
	while True:
		print('\n[*] Choose command：{1：send} {2：recv} {3：exit}')
		command = int(input('Enter command： '))	
		if command == 1:
			destHostNum  = int(input('Enter dest host num[1-3]: '))
			# global destHost
			if destHostNum == 1:
				destHost = Host_1
			elif destHostNum == 2:
				destHost = Host_2
			elif destHostNum == 3:
				destHost = Host_3
			else:
				print('Invalid host number,program exit!')
				break;

			if destHost == curr_host:
				print("Invalid host number,can't be current host,program exit!" )
				break

			msg      = input('Enter message: ')
			sourIp   = currHost['ip']
			sourPort = str(currHost['port'])	
			destIp   = destHost['ip']
			destPort = str(destHost['port'])
			# 模拟封装数据
			data     = [destIp,destPort,sourIp,sourPort,msg]
			data     = '|'.join(data)

			# 模拟物理层发送
			nextIp   = currHost['nextRoute']['ip']
			nextPort = currHost['nextRoute']['port']

			print('[*] == Send data')
			send(currHost,nextIp,nextPort,data)
		elif command == 2:
			print('[*] == Recv message')
			recv(currHost)
		elif command == 3:
			print('Exit program')
			break;
		else:
			print('Invalid command,program exit')
			break;

if __name__ == '__main__':

	if len(sys.argv) == 2:
		if sys.argv[1] == '1':
			currHost = Host_1
		elif sys.argv[1] == '2':
			currHost = Host_2
		elif sys.argv[1] == '3':
			currHost = Host_3
		else:
			print('[Usage] Only three host: 1-3')
			sys.exit(1)
		print('======== Current Host %s  %s:%d ======' %(sys.argv[1],currHost['ip'],currHost['port']))
		main(currHost)
	else:
		print('[Usage] python3 host.py [curr_host_num]')
		sys.exit(1)
	


	
