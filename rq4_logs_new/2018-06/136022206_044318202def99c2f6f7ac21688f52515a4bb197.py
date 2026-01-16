#!/usr/bin/python
import socket
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
d='y'
b=("127.0.0.1",9999)
while d=='y':
	msg=raw_input('''\n\nEnter 1.To print date
      2.To open calender
      3.To open firefox
      4.To make directory
      Choice:  ''')
	s.sendto(msg,b)			#send message to server
	p=s.recvfrom(100)	 	#receive from server
	if p[0]=='0':
		s.sendto('0',p[1])
	if p[0]=='1':
		t=raw_input("Enter name of directory or file:")
		s.sendto(t,p[1])				
	h=s.recvfrom(1000)
	print h[0]
	d=raw_input()		#Input whether user wants to send again
	s.sendto(d,p[1])

