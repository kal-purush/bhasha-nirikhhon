def sum_to(limit):
    """ returns the sum of all the primes up to limit

    args:
      limit: the number at which primes stop being collected.
    """
    return sum(primes_up_to(limit))


def primes_up_to(limit):
    """calculates all primes up to limit

    args: the upper limit for prime number calculation
    """
    def seive_next(seive, start):
        for index in range(start, limit + 1):
            if seive[index] == None:
                # mark item as prime
                seive[index] = index
                # mark multiples of that number as non prime
                for marker in range(index*2, limit + 1, index):
                    seive[marker] = 0
                return index
        return False

    # create 1 extra to prevent zero based arithmetic
    seive = [None] * (limit + 1)
    seive[0] = 0
    seive[1] = 0
    start = 2
    while start:
        start = seive_next(seive, start)
    return filter(lambda x: x != 0, seive)