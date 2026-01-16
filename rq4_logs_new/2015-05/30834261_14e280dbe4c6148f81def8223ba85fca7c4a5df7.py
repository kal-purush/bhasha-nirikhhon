def pythagorean_triplet(sum):
    """ returns the first pythagorean triplet whose lengths total sums

    args:
      sum: the amount the lengths of the triangle must total
    """
    return first(a * b * c for (a, b, c) in triplets_summing(sum))

def triplet_p(a, b, c):
    return (a * a) + (b * b) == (c * c)

def first(seq):
    return next(seq)

def triplets_summing(total):
    for a in range(total+1):
        for b in range(total+1):
            for c in range(total+1):
                if (a + b + c == total) and (a < b < c) and triplet_p(a, b, c):
                    yield (a, b, c)