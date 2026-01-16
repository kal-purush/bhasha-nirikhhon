#!/usr/bin/env python3

from argparse import ArgumentParser
import socket

def parse_file(filepath):
    """Parses a file containing output from paris-traceroute and returns a 
    list of router addresses
    """
    with open(filepath) as datafile:
        lines = datafile.readlines()

    datalines = lines[1:-1]
    path = [parse_line(line) for line in datalines]
    path = [node for node in path if node != None]
    return path

def parse_line(line):
    """Parses a single line of output from paris-traceroute"""
    # Ensure it is a hop line
    line = line.strip()
    if line.startswith("MPLS"):
        return None
   
    # Break line into parts 
    parts = line.split(' ')
    parts = [part for part in parts if part !='']
    hostname = parts[1]
    ip = parts[2].strip('()')

    # Return hostname and IP address
    if hostname == ip:
        hostname = None
    if ip == "*":
        ip = None
    return (hostname, ip)

def get_IPs(path):
    """Converts a list of router address tuples to a list of IP addresses"""
    return [ip for hostname, ip in path]

def get_hostnames(path):
    """Converts a list of router addresse tuples to a list of hostnames"""
    return [hostname for hostname, ip in path if hostname is not None]

def get_ASes(path):
    """Converts a list of IP addresses to a list of autonomous systems"""
    ASes = []

    # TODO

    return ASes

def summarize_path(path):
    """Output IP, hostnames, and ASes in path"""
    print("IPs:")
    ips = get_IPs(path)
    print('\t' + '\n\t'.join([str(ip) for ip in ips]))
    print("Hostnames:")
    print('\t' + '\n\t'.join(get_hostnames(path)))
    print("ASes:")
    print('\t' + '\n\t'.join(['%d %s' % AS for AS in get_ASes(ips)]))

def main():
    # Parse arguments
    arg_parser = ArgumentParser(description='Analyze Internet path')
    arg_parser.add_argument('-f', '--filepath', dest='filepath', action='store',
            required=True, 
            help='Path to file with paris-traceroute output')
    settings = arg_parser.parse_args()

    path = parse_file(settings.filepath)
    summarize_path(path)

if __name__ == '__main__':
    main()