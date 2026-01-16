import sys
import logging
from scapy.all import *
import pandas as pd
from tabulate import tabulate
from tqdm import tqdm
import matplotlib.pyplot as plt
import numpy as np

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def read_pcap(pcap_file):
    try:
        packets = rdpcap(pcap_file)
    except FileNotFoundError:
        logger.error(f"PCAP file not found: {pcap_file}")
        sys.exit(1)
    except Scapy_Exception as e:
        logger.error(f"Error reading PCAP file: {e}")
        sys.exit(1)
    return packets

def extract_packet_data(packets):
    packet_data = []

    for packet in tqdm(packets, desc="Processing packets", unit="packet"):
        if IP in packet:
            src_ip = packet[IP].src
            dst_ip = packet[IP].dst
            protocol = packet[IP].proto
            size = len(packet)
            packet_data.append({"src_ip": src_ip, "dst_ip": dst_ip, "protocol": protocol, "size": size})

    return pd.DataFrame(packet_data)

def protocol_name(number):
    protocol_dict = {
        1: 'ICMP', 6: 'TCP', 17: 'UDP', 
        20: 'FTP', 21: 'FTP', 22: 'SSH', 
        23: 'Telnet', 25: 'SMTP', 53: 'DNS', 
        67: 'DHCP', 68: 'DHCP', 80: 'HTTP', 
        110: 'POP3', 143: 'IMAP', 161: 'SNMP', 
        162: 'SNMP Trap', 389: 'LDAP', 443: 'HTTPS', 
        465: 'SMTPS', 587: 'SMTP', 636: 'LDAPS', 
        993: 'IMAPS', 995: 'POP3S', 1433: 'MSSQL', 
        3306: 'MySQL', 3389: 'RDP', 5432: 'PostgreSQL',  
        170: 'Print Serv',37: 'Time',120: 'CFDPTKT',
        240: 'Reserved',118: 'SQL Services',127: 'Locus PC-Interface Net Map Ser',
        94: 'IP',0: 'Reserved',19: 'CHARGEN'

    }
    return protocol_dict.get(number, f"Unknown({number})")


def analyze_packet_data(df):
    total_bandwidth = df["size"].sum()
    protocol_counts = df["protocol"].value_counts(normalize=True) * 100
    protocol_counts.index = protocol_counts.index.map(protocol_name)

    ip_communication = df.groupby(["src_ip", "dst_ip"]).size().sort_values(ascending=False)
    ip_communication_percentage = ip_communication / ip_communication.sum() * 100
    ip_communication_table = pd.concat([ip_communication, ip_communication_percentage], axis=1)

    protocol_frequency = df["protocol"].value_counts().reset_index()
    protocol_frequency.columns = ["protocol", "Count"]
    protocol_frequency["protocol"] = protocol_frequency["protocol"].map(protocol_name)

    protocol_counts_df = pd.concat([protocol_frequency.reset_index(drop=True), protocol_counts.reset_index(drop=True)], axis=1)
    protocol_counts_df.columns = ["protocol", "Count", "Percentage"]

    ip_communication_protocols = df.groupby(["src_ip", "dst_ip", "protocol"]).size().reset_index(name="Count")
    ip_communication_protocols["protocol"] = ip_communication_protocols["protocol"].map(protocol_name)

    ip_communication_protocols["Percentage"] = ip_communication_protocols.groupby(["src_ip", "dst_ip"])["Count"].apply(lambda x: x / x.sum() * 100).reset_index(drop=True)
    latency = 0 
    packet_loss = 0  

    
    throughput = total_bandwidth * 8 / (10**6)  

    return total_bandwidth, protocol_counts_df, ip_communication_table, protocol_frequency, ip_communication_protocols,latency,packet_loss,throughput

def calculate_latency(packets):
    if not packets:
        return 0

    total_latency = 0
    valid_packets = 0

    for i in range(len(packets) - 1):
        if IP in packets[i] and IP in packets[i + 1]:
            total_latency += (packets[i + 1].time - packets[i].time) * 1000  # Convert to milliseconds
            valid_packets += 1

    if valid_packets == 0:
        return 0

    return total_latency / valid_packets

def calculate_packet_loss(packets):
    if not packets:
        return 0

    expected_sequence = None
    lost_packets = 0

    for packet in packets:
        if IP in packet and hasattr(packet.payload, 'seq') and 'TCP' in packet.payload.name:
            tcp_layer = packet.payload
            if expected_sequence is None:
                expected_sequence = tcp_layer.seq
            elif hasattr(tcp_layer, 'seq') and tcp_layer.seq != expected_sequence + 1:
                lost_packets += tcp_layer.seq - expected_sequence - 1
            expected_sequence = tcp_layer.seq

    if expected_sequence is None:
        return 0

    total_packets = expected_sequence - packets[0].payload.seq + 1
    packet_loss_percentage = (lost_packets / total_packets) * 100 if total_packets > 0 else 0

    return packet_loss_percentage



def extract_packet_data_security(packets):
    packet_data = []

    for packet in tqdm(packets, desc="Processing packets for port scanning activity", unit="packet"):
        if IP in packet:
            src_ip = packet[IP].src
            dst_ip = packet[IP].dst
            protocol = packet[IP].proto
            size = len(packet)

            if TCP in packet:
                dst_port = packet[TCP].dport
            elif UDP in packet:
                dst_port = packet[UDP].dport
            else:
                dst_port = 0

            packet_data.append({"src_ip": src_ip, "dst_ip": dst_ip, "protocol": protocol, "size": size, "dst_port": dst_port})

    return pd.DataFrame(packet_data)

def detect_port_scanning(df, port_scan_threshold):
    port_scan_df = df.groupby(['src_ip', 'dst_port']).size().reset_index(name='count')
    unique_ports_per_ip = port_scan_df.groupby('src_ip').size().reset_index(name='unique_ports')
    
    potential_port_scanners = unique_ports_per_ip[unique_ports_per_ip['unique_ports'] >= port_scan_threshold]
    ip_addresses = potential_port_scanners['src_ip'].unique()
    
    if len(ip_addresses) > 0:
        logger.warning(f"Potential port scanning detected from IP addresses: {', '.join(ip_addresses)}")

def print_results(total_bandwidth, protocol_counts_df, ip_communication_table, protocol_frequency, ip_communication_protocols):
    if total_bandwidth < 10**9:
        bandwidth_unit = "Mbps"
        total_bandwidth /= 10**6
    else:
        bandwidth_unit = "Gbps"
        total_bandwidth /= 10**9

    logger.info(f"Total bandwidth used: {total_bandwidth:.2f} {bandwidth_unit}")
    logger.info("\nProtocol Distribution:\n")
    logger.info(tabulate(protocol_counts_df, headers=["Protocol", "Count", "Percentage"], tablefmt="grid"))
    logger.info("\nTop IP Address Communications:\n")
    logger.info(tabulate(ip_communication_table, headers=["Source IP", "Destination IP", "Count", "Percentage"], tablefmt="grid", floatfmt=".2f"))
    logger.info("\nShare of each protocol between IPs:\n")
    logger.info(tabulate(ip_communication_protocols, headers=["Source IP", "Destination IP", "Protocol", "Count", "Percentage"], tablefmt="grid", floatfmt=".2f"))
def plot_protocol_distribution(protocol_counts_df):
    plt.figure(figsize=(12, 8))
    plt.bar(protocol_counts_df['protocol'], protocol_counts_df['Count'], color='blue')
    plt.xlabel('Protocol')
    plt.ylabel('Count')
    plt.title('Protocol Distribution')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main(pcap_file, port_scan_threshold):

    packets = read_pcap(pcap_file)
    df = extract_packet_data(packets)
    total_bandwidth, protocol_counts_df, ip_communication_table, protocol_frequency, ip_communication_protocols,latency,packet_loss,throughput= analyze_packet_data(df)
    print_results(total_bandwidth, protocol_counts_df, ip_communication_table, protocol_frequency, ip_communication_protocols)
    latency = calculate_latency(packets)
    packet_loss = calculate_packet_loss(packets)
    if total_bandwidth < 10**9:
        bandwidth_unit = "Mbps"
        total_bandwidth /= 10**6
    else:
        bandwidth_unit = "Gbps"
        total_bandwidth /= 10**9

    logger.info(f"Latency: {latency} ms")  
    logger.info(f"Packet Loss: {packet_loss}%") 
    logger.info(f"Throughput: {throughput:.2f} {bandwidth_unit}")
    df = extract_packet_data_security(packets=packets)
    detect_port_scanning(df, port_scan_threshold)
    plot_protocol_distribution(protocol_counts_df)
    


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Please provide the path to the PCAP file.")
        sys.exit(1)
    
    pcap_file = sys.argv[1]
    default_port_scan_threshold = 100

    if len(sys.argv) >= 3:
        try:
            port_scan_threshold = int(sys.argv[2])
        except ValueError:
            logger.error("Invalid port_scan_threshold value. Using the default value.")
            port_scan_threshold = default_port_scan_threshold
    else:
        port_scan_threshold = default_port_scan_threshold
    
    main(pcap_file, port_scan_threshold)