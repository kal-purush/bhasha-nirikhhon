import sys
from cx_Freeze import setup, Executable

base = None
if (sys.platform == "win32"):
    base = "Win32GUI"

setup(
    name="OSH QR Code Generator",
    version="2.0.1",
    description="QR Code Generator from OPERATION: Sky Hogg",
    executables=[Executable("OSH QR Code Generator.py", base=base)]
)