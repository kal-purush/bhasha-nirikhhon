"""Custom topology for PGR1-SDN

Adding the 'topos' dict with a key/value pair to generate our newly defined
topology enables one to pass in '--topo=PgrTopo' from the command line.
"""

from mininet.topo import Topo

class PgrTopo( Topo ):
    "PGR1-SDN Topology"
    def __init__( self ):
        "Create custom topo"
        
        # Initialize topology
        Topo.__init__( self )

        # Add hosts
        PC0 = self.addHost('PC0')
        PC1 = self.addHost('PC1')
        PC2 = self.addHost('PC2')
        PC3 = self.addHost('PC3')
        PC4 = self.addHost('PC4')
        PC5 = self.addHost('PC5')
        PC6 = self.addHost('PC6')
        PC7 = self.addHost('PC7')
        PC8 = self.addHost('PC8')
        PC9 = self.addHost('PC9')
        PC10 = self.addHost('PC10')
        PC11 = self.addHost('PC11')
        PC12 = self.addHost('PC12')
        PC13 = self.addHost('PC13')
        PC14 = self.addHost('PC14')
        PC15 = self.addHost('PC15')
        PC16 = self.addHost('PC16')
        PC17 = self.addHost('PC17')
        PC18 = self.addHost('PC18')
        PC19 = self.addHost('PC19')
        PC20 = self.addHost('PC20')
        PC21 = self.addHost('PC21')

        # Add servers

        Server0 = self.addHost('Server0')
        Server1 = self.addHost('Server1')
        
        # Add switches
        Switch0 = self.addSwitch('Switch0')
        Switch1 = self.addSwitch('Switch1')
        Switch2 = self.addSwitch('Switch2')
        Switch3 = self.addSwitch('Switch3')
        Switch4 = self.addSwitch('Switch4')
        
        # Add links
        ##Between Switchs
        self.addLink(Switch1,Swicth0)
        self.addLink(Switch2,Swicth0)
        self.addLink(Switch4,Swicth1)
        self.addLink(Switch3,Swicth2)
        ##Between Switch-Server
        self.addLink(Server0,Swicth1)
        self.addLink(Server1,Swicth2)
        ##Between Switch-Host
        ###Switch1
        self.addLink(PC0,Swicth1)
        self.addLink(PC1,Swicth1)
        self.addLink(PC2,Swicth1)
        self.addLink(PC3,Swicth1)
        self.addLink(PC4,Swicth1)
        self.addLink(PC5,Swicth1)
        self.addLink(PC6,Swicth1)
        self.addLink(PC7,Swicth1)
        self.addLink(PC8,Swicth1)
        ###Switch2
        self.addLink(PC9,Swicth2)
        self.addLink(PC10,Swicth2)
        self.addLink(PC11,Swicth2)
        self.addLink(PC12,Swicth2)
        self.addLink(PC13,Swicth2)
        self.addLink(PC14,Swicth2)
        self.addLink(PC15,Swicth2)
        self.addLink(PC16,Swicth2)
        self.addLink(PC17,Swicth2)
        ###Switch3
        self.addLink(PC18,Swicth3)
        self.addLink(PC19,Swicth3)
        ###Switch4
        self.addLink(PC20,Swicth4)
        self.addLink(PC21,Swicth4)

topos = { 'PgrTopo': ( lambda: PgrTopo() ) }