#!/usr/bin/env python3
import os
import pathlib
import subprocess


def action(cmd, args=[]):
    filename, file_extension = os.path.splitext(cmd)
    
    if not file_extension == '.py':
        cmd = cmd + '.py'
    
    if pathlib.Path(cmd).is_file():
        args.insert(0, cmd)
        args.insert(0, "python")
        return subprocess.check_output(args)
    else:
        return "Script doesn't exist: " + cmd
