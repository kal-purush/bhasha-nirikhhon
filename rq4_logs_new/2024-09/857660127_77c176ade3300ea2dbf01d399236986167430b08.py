import math

def main():
    while True:
        print(" Choose task: "
              "1. Task 1\n2.Task2\n3.Task3\n4.Close")
        ch = int(input("Enter task "))

        if ch == 1: task1()
        elif ch == 2: task2()
        elif ch == 3: task3()
        elif ch == 4: exit()

def task1():
    N = int(input("Enter seconds "))

    return N/60

def task2():
    x = float(input())
    z = math.sqrt(16)

    down = 1 / 4 * math.sqrt(pow(math.cos(pow(x, 3)), 3))
    degrees = 0.47 * (180 / 3.14)
    up = math.cos(pow(x, 2)) * pow(math.sin(x + degrees), 2) + 1 / 3 * math.sqrt(math.log(math.fabs(x + 0.7), 4))

    result = up / down

    return result

def task3():
    a = int(input())
    b = int(input())
    c = int(input())

    check1 = pow(c, 2) == (pow(b, 2) + pow(a, 2))
    check2 = pow(a, 2) == (pow(c, 2) + pow(b, 2))
    check3 = pow(b, 2) == (pow(a, 2) + pow(c, 2))

    if bool(check1):
        return "Так, цей трикутник є прямокутним"
    elif bool(check2):
        return "Так, цей трикутник є прямокутним"
    elif bool(check3):
        return "Так, цей трикутник є прямокутним"
    else:
        return "Цей трикутник не є прямокутним"


main()