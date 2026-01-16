from datetime import date

try:
    import usocket as socket
    import utime as time
    import ustruct as struct
except:
    import socket
    import time
    import struct

# Seconds between 1900 and 1970
TIME1900TO1970 = 2208988800L

# Taken from:
# https://github.com/micropython/micropython/blob/master/esp8266/scripts/ntptime.py
def get_time(host):
    addr = socket.getaddrinfo(host, 123)[0][-1]
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(1)
    res = s.sendto('\x1b' + 47 * '\0', addr)
    msg = s.recv(48)
    s.close()
    t = struct.unpack("!I", msg[40:44])[0]
    t -= TIME1900TO1970
    return time.ctime(t).replace("  "," ")

val = get_time('pool.ntp.org')
print val