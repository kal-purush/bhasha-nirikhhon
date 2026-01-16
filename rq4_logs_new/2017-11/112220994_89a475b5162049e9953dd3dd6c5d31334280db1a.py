import sys
import re

total = 0.00

while True:
    # Prompt user to enter a filename
    print("=" * 80)
    print("Enter the file name. Type 'DONE' to quit.\n")
    print("Type 'HELP' to display commands.\n")
    print("=" * 80)
    filename = input("> ")

    # Exits out the program
    if filename.lower() == 'done':
        sys.exit()

    elif filename.lower() == 'help':
        print("=" * 80)
        print("Commands are below:")
        print("Type 'DONE' to quit.")
        print("Type 'FIND PRICES' to find all prices within file.")
        print("=" * 80)
        continue

    try:
        # Store filehandle in variable and open
        fhand = open(filename)

    except:
        print("=" * 80)
        print("Invalid input.  Please enter a command or filename.")
        print("Make sure your file is within the same directory as this program.")
        print("=" * 80)
        continue

    # Ask user what to do with the file
    print("What would you like to do with this file, %s? " % filename)
    print()
    print()
    inp = input("> ")

    # Displays the file contents
    if inp.lower() == 'open':
            print(contents.rstrip())

    # Finds all the prices within file, displays them and gives a total
    elif inp.lower() == 'find prices':

        # Regex that will find all the prices
        for line in fhand:
            p = re.findall('[0-9]+.[0-9]+', line)

            for price in p:
                print("$" + price)
                price = float(price)
                total = total + price

        print()
        print("The total is: $%s" % total)