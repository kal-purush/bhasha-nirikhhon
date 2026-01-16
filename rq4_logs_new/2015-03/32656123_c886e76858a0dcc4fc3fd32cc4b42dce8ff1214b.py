from distutils.core import setup
import py2exe, sys

sys.argv.append('py2exe')

sys.path.append('C:\\Windows\\winsxs\\x86_microsoft.vc90.crt_1fc8b3b9a1e18e3b_9.0.30729.4974_none_50940634bcb759cb\\')

setup(
    options = {'py2exe': {'bundle_files': 1, 'compressed': True}},
    windows = [{'script': "fix_vistrian_data.py"}],
    zipfile = None,
)