#!/usr/bin/env python
import sys

N=4000000

def main(argv):
  head = 1
  middle = 2
  tail = head + middle
  summ = middle
  while True:
    if tail > N:
      break
    if tail %2 == 0:
      summ += tail

    head = middle
    middle =  tail
    tail = middle + head

  print(summ)
if __name__ == '__main__':
  sys.exit(main(sys.argv))