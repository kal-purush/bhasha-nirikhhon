# We shall say that an n-digit number is pandigital if it makes use of all the digits 1 to n exactly once.
# For example, 2143 is a 4-digit pandigital and is also prime.
#
# What is the largest n-digit pandigital prime that exists?
import eulerproject_functions as ef

import itertools

nums = list("123456789")
l_pandigital_prime = None
while nums:
    permutations = [int("".join(n)) for n in itertools.permutations(nums) if list(n)[-1] in ["1", "3", "7", "9"]]

    for p in permutations[::-1]:
        if ef.is_prime(p):
            l_pandigital_prime = p
            break
    if l_pandigital_prime:
        break
    nums = nums[:-1]

print(l_pandigital_prime)