#!/usr/bin/env python3
import argparse
import os
import subprocess
import time


def ask(host, packetsize):
    cmd = ["ping", "-M", "do", host, "-c", "1", "-s", str(packetsize)]
    code = subprocess.run(cmd, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    return code.returncode

parser = argparse.ArgumentParser(
    prog = 'MTU',
    description = 'Finds MTU to host')

parser.add_argument('host', help="host, required")

args = parser.parse_args()

host = args.host

print("We are looking for MTU to", host)

Left = 1
Right = 2000

while(Right - Left > 1):
    time.sleep(1)
    Mid = (Right + Left) >> 1
    code = ask(host, Mid)
    if code == 0:
        Left = Mid
    elif code == 1:
        Right = Mid
    else:
        print("FAILURE, code: ", code)
        os._exit(0)

print("MTU to", host, "is", Left)