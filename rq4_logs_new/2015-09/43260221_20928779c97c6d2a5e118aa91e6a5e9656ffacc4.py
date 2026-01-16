# Test Script for copycat

import os
import math

def command(com):
	print(com)
	os.system(com)

# generate test files
os.system("make clean")
os.system("make shale")

print("\n---test basic functionality---")
command("./shale <test.txt")

os.system("sudo umount mountedEvil")
os.system("rm -r mountedEvil")
os.system("make clean")