import re
import csv

p = re.compile(
    r'^(?P<timestamp>[\d:.]+) IP \(.*, id (?P<id>\d+), offset (?P<offset>\d+), flags .*, proto (?P<proto>\w+) .*?, length (?P<ip_length>\d+).*\)$'
    r'(\s*^\s*(?P<src_ip>\d+.\d+.\d+.\d+).(?P<src_port>\d+) > (?P<dst_ip>\d+.\d+.\d+.\d+).(?P<dst_port>\d+):( Flags \[(?P<tcp_flags>[\w.]+)\])?(, cksum [^,]+)?(, seq (?P<seqno>\d+))?(, ack (?P<ackno>\d+))?.*$)?',
    re.M)

# tcpdump -nnv ip >> tcpdump.out
with open('tcpdump.out') as f:
    s = f.read()

with open('out.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, p.groupindex.keys())
    writer.writeheader()
    for m in p.finditer(s):
        writer.writerow(m.groupdict())