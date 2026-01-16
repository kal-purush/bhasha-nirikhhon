#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2015 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import os
import unittest

from lsst.pex import policy as pexPolicy
from lsst.obs.test import TestMapper
import lsst.utils
import lsst.utils.tests as utilsTests

class PolicyTestCase(unittest.TestCase):
    """Tests related to the use of the policy file in Butler/butlerUtils."""

    def testInRepoPolicyOverrides(self):
        """Verify that the template value specified in the policy file in the repository
        overrides the template value set in the policy file in the package.
        Checks the _parent symlink chain works.
        Checks that values not specified in the local _policy file are set with those of the package's
        _policy file.
        """

        testData = ((os.path.join('data', 'policyInRepo'),
                    os.path.join('data', 'policyInRepo', '_policy.paf')),
                    (os.path.join('data', 'policyInRepo1/a'),
                    os.path.join('data', 'policyInRepo1', 'a', '_parent', '_policy.paf')),
                    (os.path.join('data', 'policyInRepo2/a'),
                    os.path.join('data', 'policyInRepo2', 'a', '_parent', '_parent', '_policy.paf'))
            )

        for mapperRoot, actualPolicyPath in testData:
            mapper = TestMapper(root=mapperRoot)
            repoPolicyPath = os.path.join('data', 'policyInRepo', '_policy.paf')
            self.assertTrue(os.path.exists(repoPolicyPath))
            repoPolicy = pexPolicy.Policy_createPolicy(actualPolicyPath)
            template = repoPolicy.get('exposures.raw.template')
            mapperTemplate = mapper.mappings['raw'].template
            self.assertEqual(template, mapperTemplate)

            # Run a simple test case to verify that although the package's policy was overloaded with some
            # values, other values specified in the policy file in the package are loaded.
            policyPath = os.path.join('policy', 'testMapper.paf')
            policy = pexPolicy.Policy_createPolicy(policyPath)
            template = policy.get('exposures.postISRCCD.template')
            mapperTemplate = mapper.mappings['postISRCCD'].template
            self.assertEqual(template, mapperTemplate)

def suite():
    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(PolicyTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit=False):
    utilsTests.run(suite(), shouldExit)

if __name__ == '__main__':
    run(True)