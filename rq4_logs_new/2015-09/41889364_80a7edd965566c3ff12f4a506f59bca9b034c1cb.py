
import os
from pylint.lint import Run

from pylint_oca import settings


all_msg_bases = settings.ALL_BASES

EXPECTED_ERRORS = 2

paths_modules = [os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'test_repo')]

cmd = [
    '--load-plugins=pylint_oca', '-r', 'n',
    '--msg-template={path}:{line}: [{msg_id}({symbol}), {obj}] {msg}',
    '--output-format=colorized'
]
pylint_res = Run(cmd + paths_modules, exit=False)
assert sum(
    [pylint_res.linter.stats['by_msg'][msg]
     for msg in pylint_res.linter.stats['by_msg']]) == EXPECTED_ERRORS, \
    "Errors found different to expected."