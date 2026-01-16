import subprocess
import argparse
import time
import time
capture_duration = 60

# Create an argument parser to accept the IP address as an argument
parser = argparse.ArgumentParser(description="Capture DNS traffic from a specific IP address.")
parser.add_argument("ip_address", help="IP address to capture DNS traffic for")
args = parser.parse_args()

ip_address = args.ip_address

# Capture DNS traffic using tcpdump for the given IP address
capture_command = f"sudo timeout {capture_duration} tcpdump -i eth0 -n 'host {ip_address} and port 53' -s 0 -w dns_traffic_5.pcap"
subprocess.run(capture_command, shell=True)