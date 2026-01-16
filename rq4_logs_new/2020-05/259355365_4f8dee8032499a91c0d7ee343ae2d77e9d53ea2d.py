#!/usr/bin/python3

import sys
from calculator_1 import add, sub, mul, div

if __name__ == "__main__":
    argv = sys.argv[1:]

    if len(argv) != 3:
        print("Usage: ./100-my_calculator.py <a> <operator> <b>")
        exit(1)

    if argv[1] not in '+-*/':
        print("Unknown operator. Available operators: +, -, * and /")
        exit(1)

    a = int(argv[0])
    b = int(argv[2])
    op = argv[1]

    if op == '+':
        sol = add(a, b)
    elif op == '-':
        sol = sub(a, b)
    elif op == '*':
        sol = mul(a, b)
    else:
        sol = div(a, b)

    print('{} {} {} = {}'.format(a, op, b, sol))