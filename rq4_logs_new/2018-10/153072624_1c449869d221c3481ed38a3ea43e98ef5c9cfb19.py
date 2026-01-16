"""
Принимает в качестве аргументов:
1. CWL-скрипт
2. аргументы (?) для CWL-скрипта

Проверить: python hello_world.py 1st-tool.cwl echo-job.yml
"""

import subprocess
import sys

if __name__ == '__main__':
    script_path = sys.argv[1]
    args_path = sys.argv[2]
    cwl_command = ['cwl-runner', script_path, args_path]
    subprocess.call(cwl_command)