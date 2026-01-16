#!/usr/bin/env python3

"""Something like unzip, but support locale encodings"""

import zipfile
from pathlib import Path
import sys, os
import argparse

# https://docs.python.org/3.6/library/codecs.html#standard-encodings
# WinZip interprets all file names as encoded in CP437, also known as DOS Latin.

def extractZip(zfile: zipfile.ZipFile, zf: zipfile.ZipInfo, encoding: str, \
        dst_path=Path(".")):
    new_name = zf.filename.encode("cp437").decode(encoding)
    new_path = dst_path.joinpath(new_name)
    print(new_path)
    if zf.is_dir():
        new_path.mkdir(exist_ok=True)
    else:
        with new_path.open(mode='wb') as newf:
            newf.write(zfile.read(zf.filename))

def detectSubdir(zfile: zipfile.ZipFile):
    """Auto detect whether the zip file has root dir
    """
    names = zfile.namelist()
    return any(map(lambda x: names[0] in x, names))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--encode", help="encoding of input file", default="sjis")
    parser.add_argument("-d", "--dest", help="destination dir", default=".")
    parser.add_argument("-p", "--password", help="set password", default=None)
    parser.add_argument("-s", "--subdir-detect", action="store_true", \
            help="auto create dir if no root dir found", default=False)
    parser.add_argument("filename")
#    parser.add_argument("list", nargs="?")
    args = parser.parse_args()
    print(args)

    dst_path = Path(args.dest)

    zfile = Path(args.filename)
    with zipfile.ZipFile(zfile) as zf:
        if args.subdir_detect and not detectSubdir(zf):
            print("create root dir")
            dst_path = dst_path.joinpath(Path(zf.namelist()[0]))
        if args.password is not None:
            zf.setpassword(args.password)
        dst_path.mkdir(parents=True, exist_ok=True)
        for zf_one in zf.infolist():
            extractZip(zf, zf_one, args.encode, dst_path=dst_path)
    