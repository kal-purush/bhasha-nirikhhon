#!/usr/bin/python

"""
This example shows how to create an empty Mininet object
(without a topology object) and add nodes to it manually.

"""
import sys
from mininet.link import TCLink
from mininet.net import Mininet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.log import setLogLevel, info

def emptyNet():

    	"Create an empty network and add nodes to it."

	net = Mininet( controller=Controller,link=TCLink )

	info( '*** Adding controller\n' )
	net.addController( 'c0' )

	info( '*** Adding hosts\n' )
	lenh=int(sys.argv[2])
	lens=int(sys.argv[1])
	odd=1
	even=1
	swt=[]
	hosts=[]
	ipess=[]
	for j in range(lens):
		s=net.addSwitch('s'+str(j+1))
		for i in range(lenh):
		  if i%2==1:
		    print j
		    name='h'+str(j+1)+str(i+1)
		    hosts.append(name)
		    ips='10.0.0.'+str(odd)+'/24'
		    ipess.append(ips)
		    h=net.addHost(name,ip=ips)
		    odd=odd+1
		    net.addLink(h,s,bw=1)
		  else:
	            print j
		    name='h'+str(j+1)+str(i+1)
		    hosts.append(name)
	            ips='10.0.1.'+str(even)+'/24'
		    ipess.append(ips)
		    h=net.addHost(name,ip=ips)
		    even=even+1
		    net.addLink(h,s,bw=2)
 		swt.append(s)

	print hosts
	print ipess
	for i in range(lens):
		if i<lens-1:
			print "hello"	
			net.addLink(swt[i],swt[i+1],bw=2)
#	net.addLink(swt[0],swt[lens-1],bw=2)
	info( '*** Starting network\n')
	net.start()

	info( '*** Running CLI\n' )
	CLI( net )

	info( '*** Stopping network' )
	net.stop()

if __name__ == '__main__':
    setLogLevel( 'info' )
    emptyNet()