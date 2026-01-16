"""
This code test some assorted stuff that otherwise wouldn't be tested.
"""

# Standard imports.
import subprocess

# Local imports.
from install import install

###########
# TESTING #
###########

def test_install():
    """ Test that the installation process doesn't crash. """
    install()

def test_main():
    """ Test that we can run __main__.py without it crashing. """
    subprocess.run(["python3", "__main__.py"], check=True)