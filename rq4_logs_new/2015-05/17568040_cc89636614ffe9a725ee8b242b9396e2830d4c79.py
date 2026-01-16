#!/usr/bin/python

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import argparse, os, pprint, errno
import avro.schema

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', nargs=1, help='new schema', required=True, metavar='avro schema')
    parser.add_argument('-i', nargs='+', help='avro files', required=True, metavar='avro file')
    parser.add_argument('-o', nargs=1, help='output directory', required=True, metavar='output directory')
    args = parser.parse_args()

    for f in args.i:
        out = []

        try:
            os.makedirs(args.o[0])
        except OSError as e:
            if e.args[0] != errno.EEXIST:
                print os.strerror(e.args[0]), e.args[1], args.o[0]
                raise SystemExit(1)

        schema = avro.schema.parse(open(args.s[0]).read())
        writer = DataFileWriter(open(args.o[0]+'/'+f, 'w'), DatumWriter(), schema)
        reader = DataFileReader(open(f, 'r'), DatumReader())

        try:
            for i, fi in enumerate(reader):
                out.append(fi)
        except UnicodeDecodeError as e:
            pprint.pprint(e)
            break

        for o in out:
            writer.append(o)

        writer.close()

main()