#!/usr/local/bin/python3
import os

fout = os.open("python_output", os.O_WRONLY | os.O_CREAT, 0o644)

os.write(fout, bytes("Hello World", "utf-8"))
os.close(fout)