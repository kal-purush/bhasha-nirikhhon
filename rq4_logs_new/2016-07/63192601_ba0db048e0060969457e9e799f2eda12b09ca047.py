from operator import mul
from largest_adjacent_product import largest_product

def main():
    f = open('sample.in', 'r')
    s = f.read()
    chars = list(s)
    digits = [int(c) for c in chars if c != '\n']
    print largest_product(digits, 13)

if __name__ == '__main__':
    main()