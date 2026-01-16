def get_info(packet):
    if(packet.haslayer(IP)):
        packet_type = 'IP'
        ip_source = packet[IP].src
        ip_destination = packet[IP].dst
    elif(packet.haslayer(Ether)):
        packet_type = 'Ether'
        ether_source = packet[Ether].src
        ether_destination = packet[Ether].dst
    elif(packet.haslayer(TCP)):
        packet_type = 'TCP'
        tcp_source_port = packet[TCP].sport
        tcp_destination_port = packet[TCP].dport
    return packet_type,ip_source,ip_destination,ether_source,ether_destination,tcp_source_port,tcp_destination_port