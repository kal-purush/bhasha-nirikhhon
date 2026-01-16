"""
CP1404/CP5632 - Practical
Pseudocode for temperature conversion
"""

MENU = """C - Convert Celsius to Fahrenheit
F - Convert Fahrenheit to Celsius
Q - Quit"""


def main():
    print(MENU)
    choice = input(">>> ").upper()
    while choice != "Q":
        if choice == "C":
            celsius = float(input("Celsius:\n>> "))
            converted_fahrenheit = convert_celsius(celsius)
            print("Result: {:.2f} F".format(converted_fahrenheit))
        elif choice == "F":
            fahrenheit = float(input("Fahrenheit:\n>> "))
            converted_celsius = convert_fahrenheit(fahrenheit)
            print("Result: {:.2f} C".format(converted_celsius))
        else:
            print("Invalid option")
        print(MENU)
        choice = input(">>> ").upper()
    print("Thank you.")


def convert_fahrenheit(fahrenheit):
    celsius = 5 / 9 * (fahrenheit - 32)
    return celsius


def convert_celsius(celsius):
    fahrenheit = celsius * 9.0 / 5 + 32
    return  fahrenheit


main()