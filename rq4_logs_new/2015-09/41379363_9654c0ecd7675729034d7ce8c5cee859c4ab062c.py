#!/usr/bin/env python

import argparse
import subprocess
import time

import requests

parser = argparse.ArgumentParser(description='Watches over a LittleSleeper instance and runs `executable` every `cycle` seconds. The executable program will receive an integer corresponding to the increasing sound level on a scale of 0 to 11 as the first positional argument.')
parser.add_argument('url', help="URL at which LittleSleeper's JSON API can be found.", type=str)
parser.add_argument('cycle', help='frequency in seconds (supports decimals) at which LittleSleeper is queried.', type=float)
parser.add_argument('executable', help='executable program to run on every cycle. Must accept an integer value as the first positional argument.', type=str)

def run(url, cycle, executable):
    latest_value = 0
    current_level = 0
    while True:
        data = requests.get(url).json()
        current_value = data['audio_plot'][-1]
        if current_value > latest_value:
            current_level = min(current_level + 1, 11)
        else:
            current_level = max(current_level - 1, 0)
        latest_value = current_value
        executable_atoms = filter(None, executable.split(' '))
        subprocess.Popen(['nohup'] + executable_atoms + [str(current_level)])
        time.sleep(cycle)

if __name__ == '__main__':
    args = parser.parse_args()
    run(args.url, args.cycle, args.executable)
