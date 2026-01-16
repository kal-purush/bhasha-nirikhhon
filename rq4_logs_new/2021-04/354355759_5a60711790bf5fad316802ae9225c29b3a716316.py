def fibonacci(n):
    if n == 1:
        return 0
    elif n < 2:
        return 1
    else:
        return fibonacci(n-1)+fibonacci(n-2)


def main():
    while True:
        n = (input("Enter fibo digit: "))       
        try:
            n = int(n)
        except ValueError or TypeError:
            print (f"'{n}' is not a digit")
        else:
            print(fibonacci(n))
            break


if __name__ == '__main__':
    main()