from scapy.all import *
import time
import json

# Define variables for rate limiting
RATE_LIMIT_THRESHOLD = 50  # Adjust this value based on your needs
RATE_LIMIT_WINDOW = 10  # Time window in seconds

# Dictionary to store request timestamps per source IP
request_counts = {}
alerts_sent = {}  # To track whether an alert has been sent for a specific source IP

# Function to check and handle HTTP flood
def check_http_flood(pkt):
    if IP in pkt and TCP in pkt:
        src_ip = pkt[IP].src
        dst_ip = pkt[IP].dst

        if pkt[TCP].dport == 80:  # Assuming HTTP traffic on port 80
            current_time = time.time()

            if src_ip in request_counts:
                request_counts[src_ip].append(current_time)
            else:
                request_counts[src_ip] = [current_time]
            
            # Check rate limiting
            recent_requests = [t for t in request_counts[src_ip] if current_time - t <= RATE_LIMIT_WINDOW]
            
            if len(recent_requests) > RATE_LIMIT_THRESHOLD and src_ip not in alerts_sent:
                alerts_sent[src_ip] = True
                alert_info = {"src_ip": src_ip, "dst_ip": dst_ip}
                with open("alerts.json", "a") as f:
                    json.dump(alert_info, f)
                    f.write("\n")
                print(f"HTTP flood detected from {src_ip} to {dst_ip}")
                # You can add further actions here like blocking the source IP, alerting, etc.

# Sniff traffic on the network interface for 1 minute
sniff(filter="tcp", prn=check_http_flood, timeout=60)
