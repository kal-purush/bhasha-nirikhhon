import sys, re

if len(sys.argv) == 1:
    print("Usage: \n>python TagAuto.py (input file here)")
    print("Input file should be structured with singular app IDs in every line. For example:")
    print("\n---file start---\n440\n441\n29843\n3940\n33911\n9583\n--- file end ---")