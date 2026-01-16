#!/usr/bin/python
"""

Changing to use two controller for the SDX environment

SAX will be one switch and it will have its own controller
TENET will be one switch and it will have its own controller
AmLight will have multiple switches and it will have its own controller

Custom topology for AmLight/AMPATH
@author: Italo Valcy <italo@amlight.net>
@author: Renata Frez <renata.frez@rnp.br>

"""
import sys
import mininet.clean as Cleanup
from mininet.net import Mininet
from mininet.node import RemoteController
from mininet.node import OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel


def custom_topo(controller, amlight_port, sax_port, tenet_port):
    """ Create AmLight network for tests """
    # net = Mininet(topo=None, build=False)
    net = Mininet(topo=None, build=False, controller=RemoteController, switch=OVSSwitch)

    # ********************************************** TENET OXP - Start ************************************************
    TenetController = net.addController('tenet_ctrl', controller=RemoteController, ip=controller, port=tenet_port)
    TenetController.start()

    tenet_sw1 = net.addSwitch('Tenet01', listenPort=6701, dpid='cc00000000000011')
    tenet_sw2 = net.addSwitch('Tenet02', listenPort=6702, dpid='cc00000000000022')
    tenet_sw3 = net.addSwitch('Tenet03', listenPort=6703, dpid='cc00000000000033')
    net.addLink(tenet_sw1, tenet_sw2, port1=20, port2=20)
    net.addLink(tenet_sw1, tenet_sw3, port1=99, port2=99)

    h12 = net.addHost('h12', mac='00:00:00:00:00:0C')
    h13 = net.addHost('h13', mac='00:00:00:00:00:0D')
    h14 = net.addHost('h14', mac='00:00:00:00:00:0E')
    net.addLink(h12, tenet_sw1, port1=1, port2=61)
    net.addLink(h13, tenet_sw2, port1=1, port2=62)
    net.addLink(h14, tenet_sw3, port1=1, port2=63)

    # ************************************************ TENET OXP - End ************************************************

    # ************************************************ SAX OXP - Start ************************************************
    SaxController = net.addController('sax_ctrl', controller=RemoteController, ip=controller, port=sax_port)
    SaxController.start()

    sax_sw1 = net.addSwitch('Sax01', listenPort=6801, dpid='dd00000000000011')
    sax_sw2 = net.addSwitch('Sax02', listenPort=6802, dpid='dd00000000000022')
    net.addLink(sax_sw1, sax_sw2, port1=31, port2=31)

    h15 = net.addHost('h15', mac='00:00:00:00:00:0F')
    h16 = net.addHost('h16', mac='00:00:00:00:00:10')
    net.addLink(h15, sax_sw1, port1=1, port2=64)
    net.addLink(h16, sax_sw2, port1=1, port2=65)

    # ************************************************ SAX OXP - End ************************************************

    # ******************************************** AmLight OXP - Start **********************************************
    AmLightController = net.addController('amlight_ctrl', controller=RemoteController, ip=controller, port=amlight_port)
    AmLightController.start()

    Ampath1 = net.addSwitch('Ampath1', listenPort=6601, dpid='aa00000000000011')
    Ampath2 = net.addSwitch('Ampath2', listenPort=6602, dpid='aa00000000000012')
    SouthernLight2 = net.addSwitch('SoL2', listenPort=6603, dpid='aa00000000000013')
    SanJuan = net.addSwitch('SanJuan', listenPort=6604, dpid='aa00000000000014')
    AndesLight2 = net.addSwitch('AL2', listenPort=6605, dpid='aa00000000000015')
    AndesLight3 = net.addSwitch('AL3', listenPort=6606, dpid='aa00000000000016')
    Ampath3 = net.addSwitch('Ampath3', listenPort=6608, dpid='aa00000000000017')
    Ampath4 = net.addSwitch('Ampath4', listenPort=6609, dpid='aa00000000000018')
    Ampath5 = net.addSwitch('Ampath5', listenPort=6610, dpid='aa00000000000019')
    Ampath7 = net.addSwitch('Ampath7', listenPort=6611, dpid='aa00000000000020')
    JAX1 = net.addSwitch('JAX1', listenPort=6612, dpid='aa00000000000021')
    net.addLink(Ampath1, Ampath2, port1=1, port2=1)
    net.addLink(Ampath1, SouthernLight2, port1=2, port2=2)
    net.addLink(Ampath1, SouthernLight2, port1=3, port2=3)
    net.addLink(Ampath2, AndesLight2, port1=4, port2=4)
    net.addLink(SouthernLight2, AndesLight3, port1=5, port2=5)
    net.addLink(AndesLight3, AndesLight2, port1=6, port2=6)
    net.addLink(AndesLight2, SanJuan, port1=7, port2=7)
    net.addLink(SanJuan, Ampath2, port1=8, port2=8)
    net.addLink(Ampath1, Ampath3, port1=9, port2=9)
    net.addLink(Ampath2, Ampath3, port1=10, port2=10)
    net.addLink(Ampath1, Ampath4, port1=11, port2=11)
    net.addLink(Ampath2, Ampath5, port1=12, port2=12)
    net.addLink(Ampath4, Ampath5, port1=13, port2=13)
    net.addLink(Ampath4, JAX1, port1=14, port2=14)
    net.addLink(Ampath5, JAX1, port1=15, port2=15)
    net.addLink(Ampath4, Ampath7, port1=16, port2=16)
    net.addLink(Ampath7, SouthernLight2, port1=17, port2=17)

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
    net.addLink(h1, Ampath1, port1=1, port2=50)
    net.addLink(h2, Ampath2, port1=1, port2=51)
    net.addLink(h3, SouthernLight2, port1=1, port2=52)
    net.addLink(h4, SanJuan, port1=1, port2=53)
    net.addLink(h5, AndesLight2, port1=1, port2=54)
    net.addLink(h6, AndesLight3, port1=1, port2=55)
    net.addLink(h7, Ampath3, port1=1, port2=56)
    net.addLink(h8, Ampath4, port1=1, port2=57)
    net.addLink(h9, Ampath5, port1=1, port2=58)
    net.addLink(h10, Ampath7, port1=1, port2=59)
    net.addLink(h11, JAX1, port1=1, port2=60)

    # ********************************************* AmLight OXP - End ************************************************

    # ********************************************** Inter-OXP links ***********************************************
    net.addLink(Ampath1, sax_sw1, port1=40, port2=40)
    net.addLink(SouthernLight2, sax_sw2, port1=41, port2=41)
    net.addLink(tenet_sw1, sax_sw1, port1=42, port2=42)
    net.addLink(tenet_sw2, sax_sw2, port1=43, port2=43)

    # Connect AmLight switches to AmLight controller
    Ampath1.start([AmLightController])
    Ampath2.start([AmLightController])
    Ampath3.start([AmLightController])
    Ampath4.start([AmLightController])
    Ampath5.start([AmLightController])
    Ampath7.start([AmLightController])
    SouthernLight2.start([AmLightController])
    SanJuan.start([AmLightController])
    JAX1.start([AmLightController])
    AndesLight2.start([AmLightController])
    AndesLight3.start([AmLightController])

    sax_sw1.start([SaxController])
    sax_sw2.start([SaxController])

    tenet_sw1.start([TenetController])
    tenet_sw2.start([TenetController])
    tenet_sw3.start([TenetController])

    net.build()
    CLI(net)
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')  # for CLI output
    controller = sys.argv[1] if len(sys.argv) > 1 else '192.168.65.2'
    amlight_port = 6653
    sax_port = 6654
    tenet_port3 = 6655
    custom_topo(controller, amlight_port, sax_port, tenet_port3)
    Cleanup.cleanup()