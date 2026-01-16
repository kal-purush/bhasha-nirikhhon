# Functions go here
def yes_no(questions):
    valid = False
    while not valid:
        response = input(questions).lower()

        if response == "yes" or response == "y":
            response = "yes"
            return response

        elif response == "no" or response == "n":
             response = "no"
             return response

        else:
            print("please answer yes / no")

# main routine goes here
show_instructions = yes_no("Have you played this game "
                           "before? ")
print("You chose {}" .format(show_instructions))
