import generator

__author__ = 'momo9'

if __name__ == '__main__':
    l = range(10)
    it = l.__iter__()
    while True:
        try:
            print it.next()
        except StopIteration:
            break

    print '=' * 60

    g = generator.gen(2)
    it = g.__iter__()
    while True:
        try:
            print it.next()
        except StopIteration:
            break