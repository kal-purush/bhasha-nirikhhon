from elasticsearch import Elasticsearch
import argparse

import os
import sys
import logging

type='templates'
tsv_file = f"{type}.tsv"

logging.basicConfig(format='%(message)s')
logging.getLogger().setLevel(logging.INFO)
logging.Formatter('%(asctime)s %(clientip)-15s %(user)-8s %(message)s')
logging.getLogger('elastic_transport.transport').setLevel(logging.WARN)
logger = logging.getLogger(f"cat_{type}.py")

parser = argparse.ArgumentParser(description=f"Use Elasticsearch CAT APIs to dump {type} info in TSV file.")
parser.add_argument('--es-url',
                    type=str,
                    help='Elasticsearch URL')
parser.add_argument('--cert',
                    type=str,
                    help='Client certificate file to connect to Elasticsearch')
parser.add_argument('--key',
                    type=str,
                    help='Private key file for client certificate (option --cert)')
parser.add_argument('--ca',
                    type=str,
                    help='CA certificate file for client certificate (option --cert)')
parser.add_argument('--input-text-file',
                    type=argparse.FileType('r', encoding='UTF-8'),
                    help='Don\'t connect to Elasticsearch but use utf-8 content of file INPUT_TEXT_FILE instead')
args = parser.parse_args()

if not args.input_text_file and not args.es_url:
    logging.error(f"Error: either --es-url or --input-text-file is required")
    parser.print_help()
    sys.exit(1)

if not args.input_text_file:

    es = Elasticsearch(
        hosts=args.es_url,
        ca_certs=args.ca,
        client_cert=args.cert,
        client_key=args.key
    )

    all_cols = []
    for line in es.cat.templates(help=True).splitlines():
        parts = line.split('|')
        all_cols.append({
            "title": parts[0].strip(),
            "desc": parts[2].strip()
        })

    harg = ''
    for idx, col in enumerate(all_cols):
        if idx > 0:
            harg+=','
        harg+=col['title']

    lines = es.cat.templates(h=harg, v=True).splitlines()

else:

    lines = args.input_text_file.readlines()

line0 = lines[0]
last_char = 'x'
seps = []
for index in range(0, len(line0)):
    if last_char != ' ' and line0[index] == ' ': # first ' ' after title
        sep = index
        for line in lines[1:]:
            for index2 in range(sep, len(line)):
                if line[index2] == ' ': # first ' ' after value for this column
                    if index2 > sep:
                        sep = index2
                    break
        seps.append(sep)
    last_char = line0[index]

with open(tsv_file, 'w', encoding = 'utf-8') as f:
    tsv_lines = []
    for line in lines:
        for index in range(0, len(seps)):
            if index == 0:
                val = line[:seps[index]]
                f.write(val.strip())
                f.write('\t')
            elif index == len(seps) - 1:
                val = line[seps[index]:]
                f.write(val.strip())
                f.write('\n')
            else:
                val = line[seps[index-1]:seps[index]]
                f.write(val.strip())
                f.write('\t')

logging.info(f"File {tsv_file} created")