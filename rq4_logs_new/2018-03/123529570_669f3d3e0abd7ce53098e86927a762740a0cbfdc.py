import sys

"""
Generates all the partitions of a number n
"""
def partitions(n):
    # base case of recursion: zero
    if n == 0:
        yield [0]
        return

    # recurively build partition
    for p in partitions( n - 1):
        yield [1] + p 
        if p and (len(p) < 2 or p[1] > p[0]):
            yield [p[0] + 1] + p[1:]

"""
Generates all the partitions of number n, n-1,...,0
"""
def all_n_partitions(n):
    # base case of recursion: zero 
    if n == 0:
        yield [0]
        return

    # recurively build partition
    for p in all_n_partitions(n - 1):
        yield p 
        yield [1] + p 
        if p and (len(p) < 2 or p[1] > p[0]):
            yield [p[0] + 1] + p[1:]

"""
Takes an iterable of consisting of lists of number
and calculate the product and keeps the maximum using
the sum of the numbers as a key
"""
def product(numbers):
    num_map = {}
    for i in numbers:
        key = sum(i)
        x = 1
        for j in i:
            x *= j
        try :
            if x > num_map[key][1]:
                num_map[key] = (i, x)
        except KeyError:
            num_map[key] = (i, x)
    for key in num_map.keys():
        i, x = num_map[key]
        print("Number %s is the sum of %s whose product is %s" % (key, i, x))

def main():
    doall = False
    if len(sys.argv) > 2 and sys.argv[2] == 'doall':
        doall = True
    if doall:
        result = all_n_partitions(int(sys.argv[1]))
    else:
        result = partitions(int(sys.argv[1]))
    product(result)

if __name__ == '__main__':
    main()

















