#!/usr/bin/env python3

# Copyright 2013-2016 Canonical Ltd.
# Adapted from github.com/juju/juju/scripts/generate-docs.py

from argparse import ArgumentParser
import os
import sys

# Insert the directory that this module is in into the python path.
sys.path.insert(0, (os.path.dirname(__file__)))

from jujuman import JujuMan


def main(argv):
    parser = ArgumentParser()

    parser.add_argument("-a", "--also", dest="also",
                        help="set the extra see also in generated man page")
    parser.add_argument("-f", "--files", dest="files",
                        help="set the files in generated man page")
    parser.add_argument("-o", "--output", dest="filename", metavar="FILE",
                        help="write output to FILE")
    parser.add_argument("-s", "--subcommand",
                        dest="subcommand",
                        help="write man page for subcommand")
    parser.add_argument("-t", "--title", dest="title",
                        help="set the title in generated man page")
    parser.add_argument("-v", "--version", dest="version",
                        help="set the version in generated man page")
    parser.add_argument("utility",
                        help="write man page for utility")

    options = parser.parse_args(argv[1:])

    if not options.utility:
        parser.print_help()
        sys.exit(1)

    doc_generator = JujuMan(options.utility,
                            also=options.also,
                            files=options.files,
                            subcmd=options.subcommand,
                            title=options.title,
                            version=options.version)

    if options.filename:
        outfilename = options.filename
    else:
        outfilename = doc_generator.get_filename()

    if outfilename == "-":
        outfile = sys.stdout
    else:
        outfile = open(outfilename, "w")

    doc_generator.write_documentation(outfile)


if __name__ == "__main__":
    main(sys.argv)