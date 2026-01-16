# -----------------------------------------------------
# CSCI 127, Lab 8
# March 8, 2018
# Your Name
# -----------------------------------------------------


# -----------------------------------------------------
# Do not change anything below this line
# -----------------------------------------------------

def print_directory(contacts):
    print("My Contacts")
    print("-----------")
    for person in contacts:
        person.print_entry()
    print("-----------\n")

# -----------------------------------------------------

def main():
    champ = Contact("???", "Bobcat", "406-994-0000")
    president = Contact("Waded", "Cruzado", "406-994-CATS")
    professor = Contact("John", "Paxton", "406-994-4780")

    contacts = [champ, president, professor]

    print_directory(contacts)

    champ.set_first_name("Champ")
    president.set_title("President")
    professor.set_title("Professor")

    print_directory(contacts)

    print("The area code for cell number", champ.get_cell_number(), "is", \
           champ.get_area_code())

# -----------------------------------------------------

main()