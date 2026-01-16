def sum_to(limit):
    """ returns the sum of all the primes up to limit

    args:
      limit: the number at which primes stop being collected.
    """
    return sum(prime for prime in up_to(limit, primes()))


def up_to(limit, seq):
    """Generator takes tems from seq until limit is reached

    Args:
      limit: number at which seq terminates

      seq: the sequence to iterate over
    """
    for item in seq:
        if item <= limit:
            yield item
        else:
            return

def primes():
    """ Geneartor that lazily evaluates the next prime number.

    Warning: This will produce an infinte sequence"""
    def _end_of_primes_p(counter, primes):
        return counter == len(primes) - 1

    def _prime_p(num, primes):
        for prime in primes:
            if num % prime == 0:
                return False
        return True


    def _get_kval_primes(k_val, primes):
        return [x for x in [k_val*6-1, k_val*6+1] if  _prime_p(x, primes)]


    counter = 0
    primes = [2, 3]
    k_val = 1
    while True:
        while _end_of_primes_p(counter, primes):
            next_primes = _get_kval_primes(k_val, list(primes))
            k_val += 1
            for prime in next_primes:
                primes.append(prime)
        yield primes[counter]
        counter += 1