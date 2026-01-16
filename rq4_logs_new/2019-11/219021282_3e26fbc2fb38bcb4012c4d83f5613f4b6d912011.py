# advanced_square.py
# get the square of the input value


import argparse


parser = argparse.ArgumentParser(description="get the square of the input value")

parser.add_argument("square", help=" the input value whos square is needed", type=int)


arguments = parser.parse_args()

print(arguments.square**2)