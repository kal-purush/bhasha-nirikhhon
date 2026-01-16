from titanic import *
import time


def main():
    X, y = Data('train.csv').edit()
    t0 = time.time()
    Process().fit(X, y)
    print('Время выполнения процесса 1:', time.time() - t0, '\n')
    t0 = time.time()
    Process().process_two(X, y)
    print('Время выполнения процесса 2:', time.time() - t0, '\n')
    t0 = time.time()
    Process().process_three(X, y)
    print('Время выполнения процесса 3:', time.time() - t0)


if __name__ == '__main__':
    main()