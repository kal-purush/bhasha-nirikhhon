#!/usr/bin/env python
# coding:UTF-8
import time
import lib_checkip
import lib_route53

# Your AWS IAM Access Key ID
key = "XXXXXXXXXXXXXXXXXXX"
# Your AWS IAM Secret Access Key
sec = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

# SET IP(V4 / V6 / IF Name), Zone, Name, TTL
commands = (('V4', 'XXXXXXXXXXX', 'example.net', '600'),
		('V4', 'XXXXXXXXXXXXX', 'www.example.net', '600'),
		('eth0', 'XXXXXXXXXXXXXX', 'example.net', '600'))


def ip_update(key, sec, cmds):
	for cmd in cmds:
		typ = "AAAA"
		if cmd[0] == "V4":
			ip = lib_checkip.check_v4_global()
			typ = "A"
		elif cmd[0] == "V6":
			ip = lib_checkip.check_v6_global()
		else:
			ip = lib_checkip.check_v6_if(cmd[0])
		oldip = lib_route53.read_route53(key, sec, cmd[1], cmd[2], typ)
		if ip != oldip:
			lib_route53.write_route53(key, sec, cmd[1], cmd[2], typ, cmd[3], ip)
			print "Detected IP Changes: Set [" + oldip + "] to [" + ip + "]"
		time.sleep(3)


ip_update(key, sec, commands)
