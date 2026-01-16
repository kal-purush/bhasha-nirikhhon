# given a list of lists of regexen indentify which list was chosen by the user given some user input in python.
import re
import sys

#
# contact api

contacts = {}

def add_contact(phone_number,name):
    global contacts
    contacts[name] = {'phone_number':phone_number,'name':name}

def udpate_contact(old_name,new_name,phone_number):
    global contacts
    del contacts[old_name]
    contacts[new_name] = {'phone_number':phone_number,'name':new_name}

def contact_list():
    global contacts
    return contacts

def get_contact_by_name(name):
    global contacts
    if not (name in contacts):
        return None
    return contacts[name]




def main():
    # list of lists
    create = ['create','set','add']
    read = ['read', 'show', 'get']
    update = ['update', 'replace']
    delete = ['delete','remove']

    while True:
        sys.stdout.write("VOICE>")
        user_input = input()

        for word_list in (create, read, update, delete):
            for word in word_list:
                # Search through user input for word
                if re.search(word, user_input, flags=re.IGNORECASE):
                    
                    if word == 'create':
                        print("Let's create a contact.")

                        print("Phone number:")
                        phone_number = input()

                        print("Name:")
                        name = input()

                        add_contact(phone_number, name)
                        # Index 0 is the name of the list

                    
                #print("Match found in", word_list[0], "list!") 
                    
        if user_input == 'quit' or user_input == 'exit':
            break


if __name__ == "__main__":
    main()