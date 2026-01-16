import os
import sys
import unittest

import coverage


def runTest():
    os.environ['FLASK_CONFIG'] = 'testing'


    # start our coverage engine
    cov = coverage.Coverage(branch=True)
    # start measuring code coverage
    cov.start()


    # begin run test
    tests = unittest.TestLoader().discover('')
    ok = unittest.TextTestRunner(verbosity=2).run(tests).isSuccessful()


    # lets print our coverage report
    cov.stop()
    print('')
    cov.report(omit=['tests/*', 'venv*/*'])


    sys.exit(0 if ok else 1)