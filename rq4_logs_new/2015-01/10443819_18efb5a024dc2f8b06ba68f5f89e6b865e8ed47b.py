#! /usr/bin/env python3
#
# Take a list of numbers and summarize them

from math import floor

def print_descriptive_statistics(numbers):
  print("\nDescriptive Statistics: ")
  for key,value in get_descriptive_statistics(numbers).items():
    print(key + ": " + value)
  print("")

def get_descriptive_statistics(numbers):
  stats = {}
  stats["count"] = str(len(numbers))
  stats["mean"] = str(mean(numbers))
  stats["median"] = str(percentile(numbers))
  stats["97.5%"] = str(percentile(numbers, 0.975))
  stats["2.5%"] = str(percentile(numbers, 0.025))
  return stats

def mean(numbers):
  return sum(numbers)/len(numbers)

def percentile(numbers, percentile=.5):
  results = []
  sorted_numbers = sorted(numbers)

  # even, average two numbers
  if len(numbers)*percentile % 1.0 == 0.0:
    results.append(sorted_numbers[int(len(numbers)*percentile)])
    results.append(sorted_numbers[int(len(numbers)*percentile - 1)])
  else:
    results.append(sorted_numbers[int(floor(len(numbers)*percentile))])
  return mean(results)

def percentile_test():
  numbers = list(range(0,9))
  print(numbers)
  print(percentile(numbers))
  numbers = list(range(0,10))
  print(numbers)
  print(percentile(numbers))