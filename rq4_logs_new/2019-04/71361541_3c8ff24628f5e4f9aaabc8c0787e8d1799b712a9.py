#!/usr/bin/env python
from argparse import ArgumentParser

from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD

from genologics.entities import Process
import sys

DESC = """Script to copy udf from process to samples

Written by Maya Brandi, Science for Life Laboratory, Stockholm, Sweden
"""


class CopyUdfs():

    def __init__(self, process):
        self.process = process
        self.date = process.udf.get('Date delivered')
        self.samples = []

    def get_samples(self):
        all_artifacts = self.process.all_outputs(unique=True)
        for art in all_artifacts: self.samples += art.samples
        self.samples = list(set(samples))

    def set_date(self):
        for sample in self.samples:
            sample.udf['Delivered'] = self.date
            sample.put()

def main(lims, args):
    process = Process(lims, id = args.pid)
    CUdf = CopyUdfs(process)
    
    if not CUdf.date:
        sys.exit('Date delivered is not set.')
    CUdf.get_samples()
    CUdf.set_date()


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)