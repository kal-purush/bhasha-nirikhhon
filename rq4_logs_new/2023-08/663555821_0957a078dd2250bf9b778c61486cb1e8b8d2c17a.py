from scapy.all import sniff, IP, UDP
from collections import deque
import time
import json

# Parameters for flood detection
MAX_PACKETS = 100  # Number of packets to keep in the queue
TIME_INTERVAL = 2  # Time interval in seconds
THRESHOLD = 50     # Maximum number of packets within the time interval
TIMEOUT = 60       # Timeout for packet sniffing in seconds

packet_queue = deque(maxlen=MAX_PACKETS)
start_time = time.time()
udp_flood_detected = False

def packet_handler(packet):
    global packet_queue, start_time, udp_flood_detected
    
    if IP in packet and UDP in packet:
        packet_queue.append(packet)
        
        current_time = time.time()
        if current_time - start_time >= TIME_INTERVAL:
            if len(packet_queue) >= THRESHOLD and not udp_flood_detected:
                udp_flood_detected = True
                result = {
                    "alert": "UDP flood detected",
                    "source_ip": str(packet_queue[0][IP].src),
                    "destination_ip": str(packet_queue[0][IP].dst)
                }
                with open("udp_flood_alert.json", "w") as json_file:
                    json.dump(result, json_file, indent=4)
                # You can take further actions here, like logging, notifying, etc.
                
            # Reset parameters for the next interval
            packet_queue.clear()
            udp_flood_detected = False
            start_time = current_time

# Start sniffing packets
try:
    sniff(filter="udp", prn=packet_handler, timeout=TIMEOUT)
except KeyboardInterrupt:
    pass  # Allow the user to interrupt the sniffing process with Ctrl+C