"""
* Project Name : Data Structure and Algorithms Lab02.1
* Program Description :
* - arithmetic operations
* ==========================================================================
* Program History
* ==========================================================================
* Author            Date            Version     Modifications
* JH KIM            2023.03.14      v1.0        FirstWrite
"""


def calculation(x, y):
    return x + y, x - y, x * y, x / y


def main():
    print(calculation(10, 20))


if __name__ == "__main__":
    main()