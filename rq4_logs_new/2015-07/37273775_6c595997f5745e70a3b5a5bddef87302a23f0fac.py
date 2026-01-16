#!/usr/bin/env python3
import sys
import os
import csv
from time import strftime
from unicode_csv import UnicodeDictReader

# Minimum possible header.
HEADER = (
    'entity',
    'attribute',
    'value',
)

def generate(file_name, encoding, delimiter, identifier):
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

        reader = UnicodeDictReader(f, dialect=dialect, delimiter=delimiter, encoding=encoding)

        if not identifier:
            identifier = reader.fieldnames[0]

        for line in reader:
            # Assemble the identifier's fieldnames into a single string for output.
            id = build_id(line, identifier)

            # Use the identifier for each entry as the entity for the new fact(s).
            # Use the remaining fields as attributes for that entity.
            for key, value in line.items():
                w.writerow((
                    id,
                    key,
                    value,
                ))

    # Flush the remaining bits.
    sys.stdout.flush()


def main():
    usage = """Origins Delimited Generator.

    Usage:
        delimited <file_name> [--encoding=<encoding>] [--delimiter=<delimiter>] [--identifier=<indentifier>]

    Options:
        -h --help  Show usage info.
        --encoding=<encoding>  The text encoding type of the file being read.
        --delimiter=<delimiter>  The character used to separate items in the csv file.
        --identifier=<identifier>  A comma-delimited (no spaces) list of fieldnames which make up a unique identifier for an entry in the file. e.g. --identifier=first,last,birthdate
    """ #noqa

    from docopt import docopt

    # Trim './main.py delimited' from the line.
    args = docopt(usage, version='0.1')

   # Defaults
    encoding = 'utf-8'
    delimiter = ','
    identifier = None

    # Collect arg values
    file_name = os.path.join(os.getcwd(), args['<file_name>'])
    if args['--encoding'] is not None:
        encoding = args['--encoding']

    if args['--delimiter'] is not None:
        delimiter = args['--delimiter']

    if args['--identifier'] is not None:
        identifier = args['--identifier'].split(",")

    # No need to validate input because docopt makes sure all arguments are given
    generate(file_name, encoding, delimiter, identifier)


def build_id(line, identifier):
    return '_'.join([line[field] for field in identifier])

if __name__ == '__main__':
    main()