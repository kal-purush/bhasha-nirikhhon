#!/usr/bin/env python

def create():
	from mininet.net import Mininet
	from mininet.link import TCLink
	from mininet.node import CPULimitedHost
	from mininet.log import info

	net = Mininet(link=TCLink, host=CPULimitedHost)

        c0 = net.addController('c0')

        s1 = net.addSwitch('s1')

	# create virtual hosts
	h1 = net.addHost('h1', cpu=40)
	h2 = net.addHost('h2', cpu=40)

	info("Create link h1 <-> s1 ")
	l1 = net.addLink(h1, s1, loss=0, bw=10, delay="10ms", use_hfsc=True)
	info("\n")

	info("Create link h2 <-> s1 ")
	l1 = net.addLink(h2, s1, loss=0, bw=10, delay="10ms", use_hfsc=True)
	info("\n")


	return net

if __name__ == '__main__':
	from mininet.log import setLogLevel, info
	from mininet.cli import CLI

	# set the log level to get some feedback from mininet
	setLogLevel('info')

	# create the newtork
	net = create()
	# initialize the network
	net.start()
	# run the benchmarks for the two hosts

	info("Set initial congestion window on h1\n")
        net['h1'].cmd("ip route | sed 's/^/ip route change /' | sed 's/$/ initcwnd 10/' | bash")

	info("Set initial congestion window on h2\n")
        net['h2'].cmd("ip route | sed 's/^/ip route change /' | sed 's/$/ initcwnd 10/' | bash")

	CLI(net)
	# destruct the network
	net.stop()
