#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Author: Mario Stefanutti
# History:
# - 06/Nov/2017: First version
#

import argparse
from datetime import datetime, date, time
from time import sleep
import threading
import signal
import sys
import os
from threading import Lock, Thread

def interrupted(signum, frame):
    """Check Control^C and stop the program ... brutally."""

    print("STOOOOOOOP. Control^C has been pressed")
    sys.exit(0)



def process_file(input_file_name, output_file_name, delay_in_ms, line_search, line_replace):
    """Cat (echo) one file in a separated thread at the rate you specify."""

    # Open input and output file
    #
    input_file = open(input_file_name)
    output_file = open(output_file_name, 'w')

    # Print the line (after string substitution) and sleep for a while
    #
    for line in input_file:
        new_line = line.replace(line_search, line_replace)
        output_file.write(new_line)
        sleep(delay_in_ms / 1000.0)

    # Close the file
    #
    output_file.close()



def main():
    """Main."""

    # The program needs to be stopped by Control^C
    #
    signal.signal(signal.SIGINT, interrupted)

    # Read the parameters
    #
    parser = argparse.ArgumentParser(description='Echo files slowly ... or fast (see: delay_in_ms')

    parser.add_argument('-l', '--file_list', type=argparse.FileType('r'), required=True)
    parser.add_argument('-d', '--delay_in_ms', type=int, default='100')
    parser.add_argument('--file_name_search', type=str, required=True)
    parser.add_argument('--file_name_replace', type=str, required=True)
    parser.add_argument('--line_search', type=str, required=True)
    parser.add_argument('--line_replace', type=str, required=True)

    args = parser.parse_args()

    # Prepare the threads to be started
    #
    threads = []
    for file_to_read in args.file_list:
        input_file_name_temp = file_to_read.rstrip()
        output_file_name_temp = input_file_name_temp.replace(args.file_name_search, args.file_name_replace).rstrip()
        print("DEBUG - input_file_name_temp = " + input_file_name_temp + ", output_file_name_temp = " + output_file_name_temp)

        t = threading.Thread(target=process_file, args=(input_file_name_temp, \
                                                        output_file_name_temp, \
                                                        args.delay_in_ms, \
                                                        args.line_search, \
                                                        args.line_replace))
        # The threads need to be stopped by Control^C
        #
        t.setDaemon(True)
        threads.append(t)

        # Create the directory that will contain the output file if it does not exists
        #
        output_file_dir_temp = os.path.dirname(output_file_name_temp)
        print("DEBUG - os.path.dirname(output_file_name_temp) = " + os.path.dirname(output_file_name_temp))
        if output_file_dir_temp != '':
            if not os.path.isdir(output_file_dir_temp):
                os.makedirs(output_file_dir_temp)

    # Start the threads
    #
    for t in threads:
        t.start()

    # Wait for all threads to finish
    # 999999 = wait until it finishes, is needed to permit the Control^C the process
    #
    for t in threads:
        t.join(999999)

    print("DONE!")


if __name__ == '__main__':
    main()

