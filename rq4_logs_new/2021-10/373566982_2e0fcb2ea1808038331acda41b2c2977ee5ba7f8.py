#!/usr/bin/python
"""
Custom topology for AmLight/AMPATH

@author: Italo Valcy <italo@amlight.net>
@author: Renata Frez <renata.frez@rnp.br>
"""

import sys
import mininet.clean as Cleanup
from mininet.net import Mininet
from mininet.node import RemoteController
from mininet.cli import CLI
from mininet.log import setLogLevel

def custom_topo(controller, port, port2):
    """ Create AmLight network for tests """
    net = Mininet(topo=None, build=False)
    # Add switches
    Ampath1 = net.addSwitch('Ampath1', listenPort=6601, dpid='024389406000000')
    Ampath2 = net.addSwitch('Ampath2', listenPort=6602, dpid='002438af17000000')
    SouthernLight2 = net.addSwitch('SoL2', listenPort=6603, dpid='cc4e244b11000000')
    SanJuan = net.addSwitch('SanJuan', listenPort=6604, dpid='cc4e249e95000000')
    AndesLight2 = net.addSwitch('AL2', listenPort=6605, dpid='cc4e249102000000')
    AndesLight3 = net.addSwitch('AL3', listenPort=6606, dpid='0001d89ef3cf7860')
    #Sax =     net.addSwitch('s7', listenPort=6607, dpid='cc4e24967b000000')
    Ampath3 = net.addSwitch('Ampath3', listenPort=6608, dpid='00013417eb145c00')
    Ampath4 = net.addSwitch('Ampath4', listenPort=6609, dpid='00013c2c301fb300')
    Ampath5 = net.addSwitch('Ampath5', listenPort=6610, dpid='000154bf64b9f9c0')
    Ampath7 = net.addSwitch('Ampath7', listenPort=6611, dpid='00013c2c301fad00')
    JAX = net.addSwitch('JAX', listenPort=6612, dpid='154bf64b9fe50')
    # Add links
    net.addLink(Ampath1, Ampath2, port1=289, port2=289)  # 7/1 to 7/1
    net.addLink(Ampath1, SouthernLight2, port1=53, port2=52)  # 2/5 to 2/4
    #net.addLink(Ampath1, Sax, port1=290, port2=97)  # 7/2 to 3/1
    net.addLink(Ampath1, SouthernLight2, port1=290, port2=241)  # 7/2 to 6/1
    net.addLink(Ampath2, AndesLight2, port1=53, port2=49)  # 2/5 to 2/1
    #net.addLink(SouthernLight2, Sax, port1=241, port2=98)  # 6/1 to 3/2
    net.addLink(SouthernLight2, AndesLight3, port1=289, port2=233)  # 7/1 to 1/25
    net.addLink(AndesLight3, AndesLight2, port1=261, port2=98)  # 1/32 to 3/2
    net.addLink(AndesLight2, SanJuan, port1=97, port2=241)  # 3/1 to 6/1
    net.addLink(SanJuan, Ampath2, port1=193, port2=290)  # 5/1 to 7/2
    net.addLink(Ampath1, Ampath3, port1=241, port2=249)  # 6/1 to 1/29
    net.addLink(Ampath2, Ampath3, port1=338, port2=241)  # 8/2 to 1/27
    net.addLink(Ampath1, Ampath4, port1=337, port2=137)  # 8/1 to 1/1
    net.addLink(Ampath2, Ampath5, port1=337, port2=137)  # 8/1 to 1/1
    net.addLink(Ampath4, Ampath5, port1=953, port2=953)  # Po1 to Po1
    net.addLink(Ampath4, JAX, port1=177, port2=245)  # 1/11 to 1/28
    net.addLink(Ampath5, JAX, port1=177, port2=249)  # 1/11 to 1/29
    net.addLink(Ampath4, Ampath7, port1=249, port2=261)  # 1/29 to 1/32
    net.addLink(Ampath7, SouthernLight2, port1=253, port2=290)  # 1/30 to 7/2
    # Add hosts
    h1 = net.addHost('h1', mac='00:00:00:00:00:01')
    h2 = net.addHost('h2', mac='00:00:00:00:00:02')
    h3 = net.addHost('h3', mac='00:00:00:00:00:03')
    h4 = net.addHost('h4', mac='00:00:00:00:00:04')
    h5 = net.addHost('h5', mac='00:00:00:00:00:05')
    h6 = net.addHost('h6', mac='00:00:00:00:00:06')
    h7 = net.addHost('h7', mac='00:00:00:00:00:07')
    h8 = net.addHost('h8', mac='00:00:00:00:00:08')
    h9 = net.addHost('h9', mac='00:00:00:00:00:09')
    h10 = net.addHost('h10', mac='00:00:00:00:00:0A')
    h11 = net.addHost('h11', mac='00:00:00:00:00:0B')
    h12 = net.addHost('h12', mac='00:00:00:00:00:0C')
    h13 = net.addHost('h13', mac='00:00:00:00:00:0D')
    h14 = net.addHost('h14', mac='00:00:00:00:00:0E')
    h15 = net.addHost('h15', mac='00:00:00:00:00:0F')
    h16 = net.addHost('h16', mac='00:00:00:00:00:10')
    h17 = net.addHost('h17', mac='00:00:00:00:00:11')
    h18 = net.addHost('h18', mac='00:00:00:00:00:12')
    h19 = net.addHost('h19', mac='00:00:00:00:00:13')
    h20 = net.addHost('h20', mac='00:00:00:00:00:14')
    h21 = net.addHost('h21', mac='00:00:00:00:00:15')
    h22 = net.addHost('h22', mac='00:00:00:00:00:16')
    h23 = net.addHost('h23', mac='00:00:00:00:00:17')
    #h24 = net.addHost('h24', mac='00:00:00:00:00:18')
    h25 = net.addHost('h25', mac='00:00:00:00:00:19')
    h26 = net.addHost('h26', mac='00:00:00:00:00:1A')
    h27 = net.addHost('h27', mac='00:00:00:00:00:1B')
    h28 = net.addHost('h28', mac='00:00:00:00:00:1C')
    h29 = net.addHost('h29', mac='00:00:00:00:00:1D')
    h30 = net.addHost('h30', mac='00:00:00:00:00:1E')
    h31 = net.addHost('h31', mac='00:00:00:00:00:1F')
    h32 = net.addHost('h32', mac='00:00:00:00:00:20')
    h33 = net.addHost('h33', mac='00:00:00:00:00:21')
    h34 = net.addHost('h34', mac='00:00:00:00:00:22')
    h35 = net.addHost('h35', mac='00:00:00:00:00:23')
    h36 = net.addHost('h36', mac='00:00:00:00:00:24')
    h37 = net.addHost('h37', mac='00:00:00:00:00:25')
    h38 = net.addHost('h38', mac='00:00:00:00:00:26')
    h39 = net.addHost('h39', mac='00:00:00:00:00:27')
    #h40 = net.addHost('h40', mac='00:00:00:00:00:28')
    h41 = net.addHost('h41', mac='00:00:00:00:00:41')
    h42 = net.addHost('h42', mac='00:00:00:00:00:42')
    h43 = net.addHost('h43', mac='00:00:00:00:00:43')
    h44 = net.addHost('h44', mac='00:00:00:00:00:44')
    h45 = net.addHost('h45', mac='00:00:00:00:00:45')
    # Add links to switches
    net.addLink(h1, Ampath1, port1=1, port2=56)  # AtlanticWave-10G
    net.addLink(h2, Ampath1, port1=1, port2=13)  # RedClara-10G-2
    net.addLink(h3, Ampath2, port1=1, port2=8)  # perfSonar-p1p2
    net.addLink(h4, Ampath2, port1=1, port2=52)  # MIA-MI1-SW08 - eth 2/3/2
    net.addLink(h5, Ampath2, port1=1, port2=56)  # Translation Loop
    net.addLink(h6, Ampath2, port1=1, port2=11)  # FLR-10G
    net.addLink(h7, Ampath2, port1=1, port2=13)  # RedClara
    net.addLink(h8, SouthernLight2, port1=1, port2=2)  # ANSP-I2
    net.addLink(h9, SouthernLight2, port1=1, port2=3)  # perfSonar-BWCTL
    net.addLink(h10, SouthernLight2, port1=1, port2=49)  # ANSP Comm
    net.addLink(h11, SouthernLight2, port1=1, port2=51)  # RedClara 2/3
    net.addLink(h12, SouthernLight2, port1=1, port2=67)  # OF Loop
    net.addLink(h13, SouthernLight2, port1=1, port2=59)  # perfSonar-OWAMP
    net.addLink(h14, SouthernLight2, port1=1, port2=6)  # RNP
    net.addLink(h15, SouthernLight2, port1=1, port2=16)  # ANSP|XMR-2/4
    net.addLink(h16, SanJuan, port1=1, port2=194)  # UPR-100G
    net.addLink(h17, AndesLight2, port1=1, port2=51)  # perfSonar-p2p2
    net.addLink(h18, AndesLight2, port1=1, port2=52)  # REUNA|10G
    net.addLink(h19, AndesLight2, port1=1, port2=53)  # RedClara
    net.addLink(h20, AndesLight3, port1=1, port2=137)  # LSST 100G
    net.addLink(h21, AndesLight3, port1=1, port2=153)  # REUNA
    net.addLink(h22, AndesLight3, port1=1, port2=257)  # acanets-chile
    net.addLink(h23, AndesLight3, port1=1, port2=265)  # perfSonar
    #net.addLink(h24, AndesLight3, port1=1, port2=5)  # Loop
    #net.addLink(h25, Sax, port1=1, port2=1)  # FIU030WMI2FORNN
    net.addLink(h26, Ampath3, port1=1, port2=245)  # OX - 100G
    net.addLink(h27, Ampath3, port1=1, port2=149)  # Novi06 Hu1/4
    net.addLink(h28, Ampath4, port1=1, port2=954)  # MIA-MI1-RT04-ae0
    net.addLink(h29, Ampath4, port1=1, port2=955)  # MIA-MI1-RT05-ae1
    net.addLink(h30, Ampath5, port1=1, port2=257)  # jmx_sax-1/1/0
    net.addLink(h31, Ampath5, port1=1, port2=954)  # MIA-MI1-RT05-ae0
    net.addLink(h32, Ampath5, port1=1, port2=181)  # RNP-MXMIA1_et-1/0/5
    net.addLink(h33, Ampath5, port1=1, port2=253)  # FIU WS-1/3
    net.addLink(h34, Ampath5, port1=1, port2=955)  # MIA-MI1-RT04-ae1
    net.addLink(h35, Ampath5, port1=1, port2=185)  # MIA-MI1-SW09 Hu1/31
    net.addLink(h36, Ampath7, port1=1, port2=257)  # MX10003-SoL-0/1/5
    net.addLink(h37, JAX, port1=1, port2=253)  # Internet2
    net.addLink(h38, Ampath3, port1=1, port2=253)  # FLR 100G Hu1/30
    net.addLink(h39, Ampath3, port1=1, port2=229)  # FIU NAP_OPTIX Hu1/24
    #net.addLink(h40, Ampath5, port1=1, port2=7)  # JAX-CLK-SW01 Hu1/29
    net.addLink(h41, Ampath4, port1=1, port2=253)  # FIU WR1 Hu1/30
    net.addLink(h42, Ampath2, port1=1, port2=20)  # VMware02-vmnic1 eth1/20
    net.addLink(h43, AndesLight2, port1=1, port2=145)  # eth4/1 | REUNA|V. Rubin Observatory|Backup
    net.addLink(h44, Ampath5, port1=1, port2=142)  # ps-mia-public Te1/2/2
    net.addLink(h45, Ampath5, port1=1, port2=189)  # PRP-Server Hu1/14

    # Define controllers
    Domain1ctrl = net.addController('domain1ctrl', controller=RemoteController, ip=controller, port=port)
    Domain2ctrl = net.addController('domain2ctrl', controller=RemoteController, ip=controller, port=port2)

    # Connect switches to controller
    Ampath1.start([Domain1ctrl])
    Ampath2.start([Domain1ctrl])
    Ampath3.start([Domain1ctrl])
    Ampath4.start([Domain1ctrl])
    Ampath5.start([Domain1ctrl])
    Ampath7.start([Domain1ctrl])
    SouthernLight2.start([Domain1ctrl])
    SanJuan.start([Domain1ctrl])
    JAX.start([Domain1ctrl])
    AndesLight2.start([Domain2ctrl])
    AndesLight3.start([Domain2ctrl])

    net.build()
    net.start()
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')  # for CLI output
    controller = sys.argv[1] if len(sys.argv)>1 else '192.168.56.1'
    port = sys.argv[2] if len(sys.argv)>2 else '6653'
    port2 = '6654'
    custom_topo(controller, int(port), int(port2))
    Cleanup.cleanup()