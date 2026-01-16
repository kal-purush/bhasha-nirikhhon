#!/usr/bin/env python
import sys
S = set()
for line in sys.stdin:
    S.add(line.strip())
print len(S)