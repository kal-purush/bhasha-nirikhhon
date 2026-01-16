#!/usr/bin/env python

import os, re, threading
import pcapy
from threading import Thread, Lock, activeCount


# Max simultaneous threads to run
max_threads = 10

# Obtain a mutex
mutex = Lock()


def flow_monitor(protocol, node_count, run):
    xml_navn = 'flowmonitor.xml'

    flow_ids = []
    f = open('{}/{}/{}/{}'.format(protocol, node_count, run, xml_navn), 'r')

    file_contents = f.readlines()
    f.close()

    substring = "timeFirstTxPacket"

    for line in file_contents:
        if substring in line:
            flow_ids.append(line)

    data_received = 0
    packets_sent = 0
    packets_received = 0
    packets_dropped = 0
    unlisted_dropped = 0
    end_to_end_delay = 0
    average_end_to_end_delay = -1
    overhead_data = -1
    packet_drop_rate = 0

    for line in flow_ids:
        data = re.split('"|"', line)

        packets_sent += int(data[21])
        packets_received += int(data[23])
        packets_dropped += int(data[25])
        data_received = 1000 * packets_received
        end_to_end_delay += int(data[11][1:-4])

    if packets_received != 0:
        average_end_to_end_delay = end_to_end_delay / packets_received

    path = "{}/{}/{}".format(protocol, node_count, run)
    print(path)

    total_data = total_data_sent(path)

    try:
        overhead_data = (total_data/data_received) * 100
        packet_drop_rate = (1-float(packets_received)/float(packets_sent)) * 100.0
    except ZeroDivisionError as err:
        print("Field not set. Error: {}".format(err))

    if len(flow_ids) !=0:
        data_storage = '{}/{}'.format(protocol, node_count)
        histogram_storage = 'histogram_data/{}'.format(node_count)

        # Write data to histogram file
        # Delays
        mutex.acquire()
        try:
            f = open('{}histogram_delay.txt'.format(histogram_storage), 'a')
            f.write('{},'.format(average_end_to_end_delay))
            f.close()
        finally:
            mutex.release()

        # Drop rate
        mutex.acquire()
        try:
            f = open('{}HistogramDelay.txt'.format(histogram_storage), 'a')
            f.write('{},'.format(packet_drop_rate))
            f.close()
        finally:
            mutex.release()

        # Overhead
        mutex.acquire()
        try:
            f = open('{}HistogramOverhead.txt'.format(histogram_storage), 'a')
            f.write('{},'.format(overhead_data))
            f.close()
        finally:
            mutex.release()

        mutex.acquire()
        try:
            txt = ('Vi er nu i fil {}/{}\n'
                   'Total amount of packets sent {}\n'
                   'Total amount of packets received {}\n'
                   'The Average end to end delay was {} nanoseconds\n'
                   'Overhead for this test was {} percent \n'
                   'Packet drop rate for this test was {}\n'
                   '\nNew test data will come in \n\n'
                   )
            txt = txt.format(node_count, run,
                       packets_sent,
                       packets_received,
                       average_end_to_end_delay,
                       overhead_data,
                       packet_drop_rate)

            f = open('{}/Collected_data.txt'.format(data_storage), 'a')
            f.write(txt)
            f.close()
        finally:
            mutex.release()


def total_data_sent(path):
    """Gets total data received by all nodes

    :param path: Path to look for data files
    :return: The total bytes received
    """
    pcap_files = []
    total = 0

    filetype = '.pcap'

    for item in os.listdir(path):
        if item.endswith(filetype):
            pcap_files.append(item)

    for pcap in pcap_files:
        abs_path = os.path.abspath(path + '/' + pcap)

        pcap_reader = pcapy.open_offline(abs_path)
        length = int(os.popen('tshark -r {} | wc -l'.format(abs_path)).read())  # Get the number of frames

        for x in range(0, length):
            header, payload = pcap_reader.next()
            total += header.getlen()

    return total