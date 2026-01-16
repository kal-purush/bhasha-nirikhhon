import csv
import sys

reader = csv.reader(sys.stdin, delimiter='\t')
writer = csv.writer(sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)


def mapper(action, _reader, _writer):
    header = True

    for line in _reader:
        if header:
            header = False
            continue

        action(line, _writer)