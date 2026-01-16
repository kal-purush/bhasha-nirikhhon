"""Python Programs for basic Arithmatic Operations """


def add(num1, num2):
    return num1 + num2


def subtract(num1, num2):
    return num1 - num2


def multiply(num1, num2):
    return num1 * num2


def division(num1, num2):
    return num1 / num2


print("Please select operation -\n" \
      "1. Add\n",
      "2. Subtract\n",
      "3. Multiply\n",
      "4. Divide\n")

select = input("Select Operation Number")
num1 = int(input("Enter Number 1:"))
num2 = int(input("Enter Number 2:"))

if select == '1':
    print(add(num1, num2))
elif select == '2':
    print(subtract(num1, num2))
elif select == '3':
    print(multiply(num1, num2))
elif select == '4':
    print(division(num1, num2))
else:
    print("Invalid Value Entered ,Please Enter Again")


