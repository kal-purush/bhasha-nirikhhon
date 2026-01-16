from scapy.all import *

def pcap_glance(pcap_file):
    packets = rdpcap(pcap_file)  # Read PCAP file
    
    # Basic analysis
    total_packets = len(packets)
    output = f"Total packets in the capture: {total_packets}\n\n"
    
    # Extract source and destination IP addresses
    src_ips = set()
    dst_ips = set()
    
    for packet in packets:
        if IP in packet:
            src_ips.add(packet[IP].src)
            dst_ips.add(packet[IP].dst)
    
    output += f"Unique source IP addresses: {', '.join(src_ips)}\n"
    output += f"Unique destination IP addresses: {', '.join(dst_ips)}\n\n"
    
    # Check for common protocols
    protocol_counts = {}
    for packet in packets:
        if Ether in packet:
            ether_type = packet[Ether].type
            if ether_type in protocol_counts:
                protocol_counts[ether_type] += 1
            else:
                protocol_counts[ether_type] = 1
    
    sorted_protocols = sorted(protocol_counts.items(), key=lambda x: x[1], reverse=True)
    output += "Top protocols by packet count:\n"
    for protocol, count in sorted_protocols[:5]:  # Print top 5 protocols
        output += f"Protocol: {protocol} | Count: {count}\n"
    output += "\n"
    
    # Packet length analysis
    packet_lengths = [len(packet) for packet in packets]
    avg_packet_length = sum(packet_lengths) / len(packet_lengths)
    min_packet_length = min(packet_lengths)
    max_packet_length = max(packet_lengths)
    output += f"Average packet length: {avg_packet_length} bytes\n"
    output += f"Minimum packet length: {min_packet_length} bytes\n"
    output += f"Maximum packet length: {max_packet_length} bytes\n\n"
    
    # Timestamp analysis
    if packets:
        start_time = packets[0].time
        end_time = packets[-1].time
        duration = end_time - start_time
        output += f"Capture start time: {start_time}\n"
        output += f"Capture end time: {end_time}\n"
        output += f"Capture duration: {duration} seconds\n\n"
    
    # Port scanning detection
    src_ports = {}
    for packet in packets:
        if TCP in packet:
            src_ip = packet[IP].src
            src_port = packet[TCP].sport
            if src_ip not in src_ports:
                src_ports[src_ip] = set()
            src_ports[src_ip].add(src_port)
    
    potential_port_scanners = []
    for src_ip, ports in src_ports.items():
        if len(ports) > 100:  # Example threshold for port scanning (adjust as needed)
            potential_port_scanners.append((src_ip, list(ports)))
    
    if potential_port_scanners:
        output += "Potential Port Scanners:\n"
        for idx, (ip, ports) in enumerate(potential_port_scanners, start=1):
            output += f"Source IP: {ip}\n"
            output += f"Scanned Ports: {', '.join(map(str, ports))}\n"
            output += "\n"
    
    return output

if __name__ == "__main__":
    pcap_file = "sample.pcap"  # Replace with your actual PCAP file path
    analysis_result = pcap_glance(pcap_file)
    print(analysis_result)