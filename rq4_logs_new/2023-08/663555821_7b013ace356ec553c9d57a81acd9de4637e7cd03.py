from scapy.all import sniff, IP, ICMP
import time
import json

# Initialize variables for flood detection
packet_count = 0
packet_queue = []

# Rate limit and notification threshold
rate_limit = 50  # Packets per second
notification_threshold = 60  # Notify if the rate exceeds this count

# JSON file to log flood events
json_filename = "icmp_flood_log_1.json"

# Function to log flood events to JSON file
def log_flood_event(src_ip, dst_ip):
    flood_info = {"src_ip": src_ip, "dst_ip": dst_ip, "timestamp": time.time()}
    with open(json_filename, "a") as f:
        json.dump(flood_info, f)
        f.write("\n")

# Function to process captured packets
def process_packet(packet):
    global packet_count, packet_queue

    if packet.haslayer(ICMP):
        icmp_packet = packet[ICMP]
        if icmp_packet.type == 8:  # ICMP Echo Request (ping)
            packet_count += 1
            packet_queue.append(time.time())

            # Remove timestamps older than 1 second from the queue
            current_time = time.time()
            packet_queue = [ts for ts in packet_queue if current_time - ts <= 1]

            # Check if the packet rate exceeds the limit
            if len(packet_queue) >= rate_limit:
                print("ICMP flood rate exceeded!")
                if packet_count >= notification_threshold:
                    print("Notifying user...")  # Add your notification code here
                    # Assuming you want to log only if the flood event is severe
                    log_flood_event(packet[IP].src, packet[IP].dst)

                # Reset packet count and queue
                packet_count = 0
                packet_queue.clear()

# Sniff network traffic and call the process_packet function
# Filter for ICMP traffic (adjust filters as needed)
sniff(filter="icmp", prn=process_packet, timeout=60)