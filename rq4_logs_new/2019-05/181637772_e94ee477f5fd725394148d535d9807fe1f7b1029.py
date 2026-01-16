#!/usr/bin/env python3

import os
import subprocess
from threading import Thread, activeCount
import time
import argparse
from shutil import rmtree

max_threads = 4
protocol_types = ['OLSR']  # ['AODV', 'OLSR', 'DSR', 'DSDV']
data_rates = []

parser = argparse.ArgumentParser(description='construct connectivity matrices')
parser.add_argument('--ns3_path', help='The path to your ns3 root directory', default='.')
parser.add_argument('--clean', default=False, action='store_true', help='Remove generated files')
args = parser.parse_args()

nodes_cfg = {'start': 140, 'stop': 140, 'step': 10}
runs = 100
signal_strength = -12

# With regards to different starting parameters a small list will be gone through and what they refer to.
# changes in the script may be required in order to change the parameter of interest.

# Run_number refers to which run which means its required to make some sort of statistic on a set of parameters.
# File_name is where the files should be saved as the "root" of the structure.
# Placing the Overheadthreading.py in the root folder
# Will allow it to be run
# MaxChildren refers to the amount of nodes that are allowed to transmit at the same time.
# Setting it to 1 or 0 will only let
# The main traffic generator work anything beyond will increase the amount
# of transmitters through allowing GenerateTrafficChild to be run
# packetSize will alter the size of the packet, if changing this alterations are required in the Overhead.py script
# Will look into this later on.
# numPacket changes the amount of packets sent during a run
# min_random_interval refers to the highest number the random interval will generate,
# could be of interest for higher packetintervals
# max_random_interval refers to the lowest number the random interval will generate
# min_packetinterval is practically added to the max packetinterval, can be a float if of interest
# max_packetinterval is almost the max however min packetinterval is added to it later on.
# This number MUST be an integer due to modulus


def run_simulation(dir_name, run_number, node_count, protocol):
    # Error at times occur where some of the threads will terminate.
    # threads will create the directory but when the simulation normally would be done
    # nothing can be found in the directory

    start = time.time()

    if not os.path.isdir('{}/time'.format(dir_name)):
        os.makedirs('{}/time'.format(dir_name))

    command = [
        './waf', '--run',
        ('manetgwonly '
         '--Run_number={} '
         '--File_name={} '
         '--numNodes={} '
         '--protocol={} '
         '--SignalStrenght={} '
         ).format(run_number,
                  dir_name,
                  node_count,
                  protocol,
                  signal_strength)
    ]

    print(' '.join(command))
    subprocess.call(command)

    end = time.time()
    days, hours, minutes, seconds = seconds_to_parts(end - start)
    print('{} node {} simulation {} took {}d {}h {}m {:.2f}s\n'.format(node_count,
                                                                       protocol,
                                                                       run_number,
                                                                       int(days),
                                                                       int(hours),
                                                                       int(minutes),
                                                                       seconds))


def seconds_to_parts(seconds):
    day = seconds // (24 * 3600)
    seconds %= (24 * 3600)

    hour = seconds // 3600
    seconds %= 3600

    minutes = seconds // 60
    seconds %= 60

    return day, hour, minutes, seconds


def auto_test():
    for protocol in protocol_types:
        root = "Results/" + protocol

        if not os.path.isdir(root):
            os.makedirs(root)
        for node_count in range(nodes_cfg['start'], nodes_cfg['stop']+1, nodes_cfg['step']):
            dir_name = '{}/{}{}'.format(root, protocol, node_count)
            for i in range(runs):
                run_number = i+1
                run_name = '{}/test{}'.format(dir_name, run_number)
                while True:
                    time.sleep(2)
                    if activeCount() <= max_threads:
                        thread = Thread(target=run_simulation, args=(run_name, run_number, node_count, protocol))
                        thread.start()
                        break


# function from:
# https://github.com/ActiveState/code/tree/5cf284892339de263d760babf6a15a9675843d94/recipes/Python/577058_query_yesno
def query_yes_no(question, default="yes"):
    """Ask a yes/no question via input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '{}'".format(default))

    while True:
        print(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def clean():
    if not os.path.isdir('Results'):
        print('Nothing to clean')
        return

    if query_yes_no('Delete ' + os.path.abspath('Results'), 'no'):
        print('Deleting ' + os.path.abspath('Results'))
        rmtree('Results')
    exit(0)


def ensure_optimized():
    mode = subprocess.check_output(['./waf', '--check-profile']).split()[-1]

    if mode == 'debug':
        print('Switching to optimised ns3 build profile. This takes a while the first time')
        time.sleep(5)
        subprocess.call(['./waf', 'configure', '--build-profile=optimized', '--out=build/optimized'])
        subprocess.call(['./waf', 'build'])


os.chdir(args.ns3_path)

if args.clean:
    clean()
    exit(0)

ensure_optimized()
auto_test()