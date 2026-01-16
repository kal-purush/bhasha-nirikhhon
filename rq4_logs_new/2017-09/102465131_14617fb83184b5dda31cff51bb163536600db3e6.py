class Person:

    def __init__(self, name, number, address, gender, email):
        self.name = name
        self.number = number
        self.address = address
        self.gender = gender
        self.email = email

    def __repr__(self):
        return ("Name: {} Number: {} Address: {} Gender: {} Email: {}".format(self.name, self.number, self.address,
                                                                              self.gender, self.email))


class ContactManager:

    def __init__(self, contacts=[]):
        self.contacts = contacts

    def add_contact(self, contact):
        if contact in self.contacts:
            return False
        else:
            self.contacts.append(contact)
            return True

    def remove_contact(self, name):
        for contact in self.contacts:
            if contact.name == name:
                self.contacts.remove(contact)
                return contact
            return None

    def get_size(self):
        return len(self.contacts)

    def save(self):
        with open("contacts.txt", 'w') as ch:
            for i in self.contacts:
                ch.write(str(i.__dict__))

            ch.close()


    def loop(self):

        condition = True
        while condition:

            choice = input("select one: 'add_contact' 'delete_contact' 'search_contact', get_len  exit: ")

            if choice == 'add_contact':
                contact_name = str(input("enter name for addition: "))
                number = int(input("enter {}'s number: ".format(contact_name)))
                address = str(input("enter the address:"))
                email = str(input("enter the email address:"))
                gender = str(input("enter male or female: "))

                cool = Person(name=contact_name, number=number, address=address, email=email, gender=gender)
                if self.add_contact(cool):
                    print("contact has been added successfully")
                else:
                    print("Not added!")

                for contact in self.contacts:
                    print(contact)

            elif choice == "delete_contact":
                contact_input = input("enter name for deletion:")
                for contact in self.contacts:
                    if contact.name == contact_input:
                        self.remove_contact(contact.name)
                        print(contact)

                    else:
                        print("Life is tasty")

            elif choice == "search_contact":
                search_query = input("enter name for search: ")
                for contact in self.contacts:
                    if contact.name == search_query:
                        print("**{} - {}: exists in directory".format(search_query, contact.number))
                    else:
                        print("name doesn't exist")

            elif choice == "get":
                print(self.get_size())

            elif choice == "exit":
                self.save()
                condition = False

"""initialize dummy contact"""
ibrahim = Person(name="ibrahim", number="09876544", address="no20", gender="M", email="oladelfg" )

"""initiaize address_book"""
address_book = ContactManager()

"""address_book loop"""
address_book.loop()
