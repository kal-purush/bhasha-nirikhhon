#!/usr/bin/env python3

# Created by Jenoe Balote
# Created on May 2021
# This program converts celsius to fahrenheit

def convert_degrees():
    # input
    celsius_string = str(input("Enter temperature in (°C): "))

    # process & output
    try:
        celsius_degree = int(celsius_string)
        fahrenheit = (9/5) * celsius_degree + 32
        print("\n{0}°C is equal to {1}°F.".format(celsius_degree, fahrenheit))
    except Exception:
        print("\nInvalid.")
    finally:
        print("\nThanks for participating!")


def main():
    # this function calls other functions

    # call function
    convert_degrees()


if __name__ == "__main__":
    main()