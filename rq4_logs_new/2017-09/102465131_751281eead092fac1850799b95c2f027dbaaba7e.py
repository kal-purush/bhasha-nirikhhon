class ContactManager:
    pass

    def __init__(self, name, number, gender, email, address):
        self.name = name
        self.number = number
        self.gender = gender
        self.email = email
        self.address = address

    def set_name(self, name):
        self.name = name
        return name

    def set_number(self, number):
        self.number = number
        return number

    def set_gender(self, gender):
        self.gender = gender
        return gender

    def set_email(self, email):
        self.email = email
        return email

    def set_address(self, address):
        self.address = address
        return address

name = str(input('what is your name?'))
number = int(input('what is your phone number?'))
gender = str(input('what is the gender?'))
email = str(input('enter the email:'))
address = str(input("enter the address:"))

contact = ContactManager(name, number, gender, email, address)

print(contact.set_name(name))
print(contact.set_address(address))
print(contact.set_gender(gender))
print(contact.set_email(email))
print(contact.set_number(number))
