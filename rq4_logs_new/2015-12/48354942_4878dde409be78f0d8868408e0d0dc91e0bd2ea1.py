import random

def uniform_sampling(s, bottom, top, blacklist):
  # s is the index of whitelisted number to be sampled.
  s = random.randint(bottom, top - len(blacklist))
  s -= bottom
  hi = len(blacklist) - 1
  lo = 0
  while True:
    if (lo > hi):
      return bottom + s

    mid = int((hi + lo) / 2)
    # below is the number of whitelisted integers below mid.
    below = blacklist[mid] - bottom - (mid - lo);
    if (below > s):
      top = blacklist[mid] - 1;
      hi = mid - 1;
    elif (below <= s):
      bottom = blacklist[mid] + 1;
      lo = mid + 1;
      s -= below

for i in range(8):
  print (i, uniform_sampling(i, 0, 10, [2, 5, 9]))

for i in [2, 3, 4, 5, 6]:
  print (i, uniform_sampling(i, 2, 9, [2, 4, 5]))