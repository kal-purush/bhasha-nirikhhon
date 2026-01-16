#!/usr/bin/python
from pathlib import Path
from datetime import datetime

BUPY_INCL = Path(__file__).parent / 'bupy_include'
BUPY_EXCL = Path(__file__).parent / 'bupy_exclude'
SRC_DIRS = BUPY_INCL.read_text().splitlines()
EXCLUSIONS = BUPY_EXCL.read_text().splitlines()

def backup_init():
    if not Path('/run/media/erik/wd_backup').exists():
        print('Could not locate wd_backup drive in /run/media/erik')
        return 1
    dest_drive = Path('/run/media/erik/wd_backup')
    dest_dir = dest_drive / 'bupy'
    if not dest_dir.exists():
        dest_dir.mkdir()
    bupy_time = datetime.now().isoformat(timespec='seconds')
    dest = dest_dir / bupy_time
    dest.mkdir()

# test exclusions for /
# if it has a slash, it's a path; otherwise, it's a name
# either way, if it has a glob, use Path.match(glob-pattern)!