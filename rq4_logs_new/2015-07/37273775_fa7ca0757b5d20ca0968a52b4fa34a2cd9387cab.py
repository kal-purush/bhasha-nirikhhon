#!/usr/bin/env python3
import sys
import os
import csv
import time
from unicode_csv import UnicodeCsvReader

# Minimum possible header.
HEADER = (
    'entity',
    'attribute',
    'value',
)

def generate(file_name, encoding, delimiter):
    # Creating a CSV writer that feeds to stdout.
    w = csv.writer(sys.stdout)

    # Write the header to start.
    w.writerow(HEADER)

    # Create each fact
    with open(file_name, 'rU') as f:
        sniff = 1024
        dialect = None

        # Infer various properties about the file
        # Sample the file to determine the dialect
        sample = '\n'.join([l for l in f.readlines(sniff)])
        f.seek(0)

        # Determine dialect
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample)

        reader = UnicodeCsvReader(f, dialect=dialect, delimiter=delimiter, encoding=encoding)
        header = reader.next()

        for i, field in enumerate(header):
            w.writerow([
                field,
                'label',
                field
            ])
            w.writerow([
                field,
                'index',
                i
            ])

    # Flush the remaining bits.
    sys.stdout.flush()

def main():
    usage = """Origins Delimited Schema Generator.

    Usage:
        delimited-schema <file_name> [--encoding=<encoding>] [--delimiter=<delimiter>]

    Options:
        -h --help  Show usage info.
        --encoding=<encoding>  The text encoding type of the file being read.
        --delimiter=<delimiter>  The character used to separate items in the csv file.
    """ #noqa

    from docopt import docopt

    # Trim './main.py' from the line.
    args = docopt(usage, version='0.1')

   # Defaults
    encoding = 'utf-8'
    delimiter = ','

    # Collect arg values
    file_name = os.path.join(os.getcwd(), args['<file_name>'])

    if args['--encoding'] is not None:
        encoding = args['--encoding']

    if args['--delimiter'] is not None:
        delimiter = args['--delimiter']

    # No need to validate input because docopt makes sure all arguments are given
    generate(file_name, encoding, delimiter)

if __name__ == '__main__':
    main()