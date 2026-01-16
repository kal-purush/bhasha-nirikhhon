# Creating functions for the calculator using the math library
import math

def addition(number1, number2):
    """
    This function adds two numbers together.

    Args:
    number1 (float): The first number to be added.
    number2 (float): The second number to be added.

    Returns:
        float: The sum of the two numbers.
    """
    result = number1 + number2
    return result

def subtraction(number1, number2):
    """
    This function subtracts two numbers.

    Args:
        number1 (float): The first number to be subtracted.
        number2 (float): The second number to be subtracted.

    Returns:
    float: The difference of the two numbers.
    """
    result = number1 - number2
    return result

def multiplication(number1, number2):
    """
    This function multiplies two numbers.

    Args:
    number1 (float): The first number to be multiplied.
    number2 (float): The second number to be multiplied.

    Returns:
    float: The product of the two numbers.
    """
    result = number1 * number2
    return result

def division(number1, number2):
    """
    This function divides two numbers.

    Args:
    number1 (float): The first number to be divided.
    number2 (float): The divisor number.

    Returns:
    float: The result of the two numbers.
    """
    result = number1 / number2
    return result

def main():
    """
    Main function to interact with the user and perform 
    mathematical operations.
    """

    while True:
        print("\n--- Basic Calculator ---")
        print("1. Addition")
        print("2. Subtraction")
        print("3. Multiplication")
        print("4. Division")
        print("0. Exit")

        option = int(input("Enter the desired option: "))

        if option == 0:
            # Exit the program
            break

        elif option in [1, 2, 3, 4]:
            # Get the two numbers from the user
            number1 = float(input("Enter the first number: "))
            number2 = float(input("Enter the second number: "))

            # Perform the operation based on the user's choice
            if option == 1:
                result = number1 + number2
                operation = "addition"
            elif option == 2:
                result = number1 - number2
                operation = "subtraction"
            elif option == 3:
                result = number1 * number2
                operation = "multiplication"
            else:
                if number2 == 0:
                    print("Division by zero is not possible")
                    continue # Return to main menu without a result to avoid errors
                else:
                    result = number1 / number2
                    operation = "division"

            # Display the result
            print(f"{number1} {operation} {number2} = {result}")
        
        else:
            # Handle invalid input
            print("Invalid option. Please enter a number between 0 and 4.")

if __name__ == "__main__":
    main()
