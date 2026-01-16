#!/usr/bin/env python

import sys
import unittest
from pkg_resources import resource_filename
from charmhelpers.fetch.archiveurl import ArchiveUrlFetchHandler 

# allow importing actions from the hooks directory
sys.path.append(resource_filename(__name__, '../hooks'))

from install import DEBS, full_url


class TestHashesAndFiles(unittest.TestCase):

    def test_hashes_and_files(self):

        h = ArchiveUrlFetchHandler()

        for deb, sha in DEBS.iteritems():
            try:
                h.download_and_validate(full_url(deb), sha, validate="sha1")
            except Exception as e:
                self.fail("download and validate failed: " + str(e))

if __name__ == '__main__':
    unittest.main()