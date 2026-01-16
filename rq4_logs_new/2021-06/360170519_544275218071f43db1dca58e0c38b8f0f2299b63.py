import magic
import hashlib
import os
import glob

def calculate_checksum(filenames):
    for fn in filenames:
        with open(fn, 'rb') as inputfile:
            data = inputfile.read()
            print("******************" + fn + "******************")
            print("  SHA 1: " + hashlib.sha1(data).hexdigest())
            print("SHA 256: " + hashlib.sha256(data).hexdigest())
            print("    MD5: " + hashlib.md5(data).hexdigest())

if __name__ == "__main__":
    files = glob.glob('./*')
    calculate_checksum(files)