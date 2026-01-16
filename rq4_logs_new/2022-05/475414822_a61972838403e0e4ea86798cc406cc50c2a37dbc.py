from multiprocessing import Pool
from math import log
import argparse


def raiz(num):
    return int(num) * 2 # replace with heavy computation

def pot(num):
    return pow(num,num)

def logFn(num):
    return log(num)

def main(args):
    funcs = {'raiz': raiz, 'pot': pot, 'log': logFn}
    pool_size = int(args.proc)
    file_directory = args.path
    fn_to_run = funcs[args.func]    

    p = Pool(processes=pool_size)

    result = []
    with open(file_directory, 'r') as f:
        matrix = [[int(num) for num in line.split(',')] for line in f if line.strip() != "" ]
        print(f'\nMatriz original {matrix}')
        for row in matrix:
            result.append(p.map(fn_to_run, row))
    print(f'\nMatriz resultado {result}\n')


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-p", '--proc', required=True,
                        help='entero indica cantidad de procesos')
	parser.add_argument("-f", '--path', required=True,
                        help='path donde se guarda el archivo')
	parser.add_argument('-c', '--func', dest='func',
                        choices=['raiz', 'pot', 'log'], 
                        required=True,
                        help='indica tipo de funcion')
	args = parser.parse_args()
	main(args)