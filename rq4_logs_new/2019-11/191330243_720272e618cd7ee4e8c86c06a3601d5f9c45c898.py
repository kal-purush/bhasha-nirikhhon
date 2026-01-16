"""
Run this file automate compiling of execuatables.
"""
from subprocess import run


# Run cmds to compile for both Windows & Linux
run(['pyinstaller', 'main.py', '-F', '-n', 'Conversions'])
run(['wsl', 'sudo', 'pyinstaller', 'main.py', '-F', '-n', 'Conversions'])