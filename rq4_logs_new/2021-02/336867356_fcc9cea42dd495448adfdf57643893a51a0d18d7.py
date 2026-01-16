# coding: utf-8

__version__ = 1.0

import sys
import os
import argparse


argpar = argparse.ArgumentParser(description=f"Bullshit File Generator | v. {__version__}")
argpar.add_argument("size", help="set file size. use K, M, G (e.g. 512K, 1M, 23G)", metavar="size", type=str)
argpar.add_argument("filename", help="set your filename", type=str)


class SizeError(Exception):
    def __init__(self) -> None:
        super().__init__("Invalid size defined. Please check.")


class BSFG(object):
    def __init__(self) -> None:
        self._fsize = None
        self._fname = None

    def setFilesize(self, fsize: str) -> object:
        self._fsize = self._parseFilesize(fsize)
        return self

    def setFilename(self, fname: str) -> object:
        self._fname = fname
        return self

    def _parseFilesize(self, fsize: str) -> int:
        fsize = str(fsize).lower()
        multiplier = None
        size_num = fsize[:-1]
        size_id = fsize[-1]
        if not size_num:
            raise SizeError
        if size_id == "k":
            multiplier = 1024
        elif size_id == "m":
            multiplier = 1024 * 1024
        elif size_id == "g":
            multiplier = 1024 * 1024 * 1024
        else:
            raise SizeError
        return int(size_num) * multiplier

    def _createFile(self) -> None:
        chunk_size = 65535
        with open(self._fname, "wb") as fh:
            perc = 0
            written_bytes = 0
            while True:
                print(f"Writing file {self._fname} of {self._fsize} bytes... {perc}%", end="\r", flush=True)
                randbytes = os.urandom(chunk_size)
                if written_bytes > 0:
                    perc = round(100 / self._fsize * written_bytes)
                fh.write(randbytes)
                written_bytes += chunk_size
                if written_bytes >= self._fsize:
                    print("")
                    print("done.")
                    break


    def run(self) -> None:
        self._createFile()


if __name__ == "__main__":
    cliargs = argpar.parse_args()
    
    try:
        script = BSFG()
        script.setFilesize(cliargs.size)
        script.setFilename(cliargs.filename)
        script.run()
    except KeyboardInterrupt:
        print("")
        print("Program aborted by user.")
        sys.exit(0)
    except SizeError as e:
        print(e)
        sys.exit(1)
    except Exception as e:
        raise
    
    sys.exit(0)