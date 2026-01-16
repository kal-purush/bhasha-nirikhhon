def calcuate(num1:float, num2:float, operation:str):

    """
    num1 is the first argument, it shouldbe a number
    num2 is the second argument, it shpuld be a number also
    parameter operation should be either  + _ * /
    return a float ouput


    """

    if operation.lower() == 'add':
        result = num1 + num2
    elif operation.lower() == 'subtract':
        result = num1 - num2
    elif operation.lower() == 'multiply':
        result = num1 * num2
    elif operation.lower() == 'divide':
        result = num1 / num2

    else:
        raise Exception
    return result

if __name__ '__main__':
    number_1 = float(input("Please enter the first number"))
    number_2 = float(input("Please enter the second number"))
    operator_input = ("Please enter the opration you wish to usein the equation choose from the following: add, subtract, multiply, divide")