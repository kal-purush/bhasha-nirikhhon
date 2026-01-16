import re
from collections import UserDict
from datetime import datetime
import pickle
import os


# Get the directory where main.py is located
current_directory = os.path.dirname(os.path.abspath(__file__))

# Specify the file name
file_name = "contacts.pkl"

# Construct the full file path
file_path = os.path.join(current_directory, file_name)

# You can use 'file_path' to load or save files in the same directory as main.py



def save_contacts(contacts_dict):
    try:
        with open(file_path, 'wb') as file:
            pickle.dump(contacts_dict, file)
        print("Contacts saved")
    except Exception as e:
        print(f"An error occurred while saving contacts: {e}")


def load_contacts(contacts_dict):
    try:
        if os.path.exists(file_path):
            with open(file_path, 'rb') as file:
                loaded_data = pickle.load(file)
                contacts_dict.data = loaded_data.data
            print(f"Loaded {len(contacts_dict)} contacts from {file_path}.")
        else:
            print("The specified file does not exist.")
    except Exception as e:
        print(f"An error occurred while loading contacts: {e}")


def search_contacts(contacts_dict, search_term):
    results = []
    for contact in contacts_dict.values():
        if (
            search_term.lower() in contact.name.value.lower() or
            any(search_term.lower() in field.value.lower() for field in contact.fields)
        ):
            results.append(contact)
    return results

          
class Field:
    def __init__(self, value):
        self._value = value

    def __str__(self):
        return str(self._value)

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        self._value = new_value

class Name(Field):
    def __init__(self, value):
        super().__init__(value)

class InvalidPhoneNumberError(Exception):
    def __init__(self, phone_number):
        super().__init__(f"Invalid phone number format: {phone_number}")

class Phone(Field):
    def __init__(self, value):
        if not self.is_valid_phone(value):
            raise InvalidPhoneNumberError(value)
        super().__init__(value)

    @staticmethod
    def is_valid_phone(value):
        # Видаляємо всі не цифрові символи і перевіряємо, чи залишається рядок з 10 або більше цифр
        numeric_value = re.sub(r'\D', '', value)
        return len(numeric_value) >= 10

class Birthday(Field):
    def __init__(self, value, birthdate=None):
        super().__init__(value)
        if birthdate is not None:
            self._birthdate = birthdate
        else:
            self._birthdate = self.parse_birthday(value)

    def parse_birthday(self, value):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("Invalid date format")

    def get_value(self):
        return self._birthdate.strftime("%Y-%m-%d")

    @property
    def value(self):
        return self.get_value()

    @value.setter
    def value(self, new_value):
        if new_value:
            self._birthdate = self.parse_birthday(new_value)
        else:
            self._birthdate = None

    def days_to_birthday(self):
        if self._birthdate is None:
            return None

        today = datetime.now().date()
        next_birthday = datetime(today.year, self._birthdate.month, self._birthdate.day).date()
        if today > next_birthday:
            next_birthday = datetime(today.year + 1, self._birthdate.month, self._birthdate.day).date()
        days_left = (next_birthday - today).days
        return days_left

class Record:
    def __init__(self, name, birthday=None):
        self.name = Name(name)
        self.fields = []
        if birthday:
            self.add_field(Birthday(birthday))

    def add_field(self, field):
        if isinstance(field, Field):
            self.fields.append(field)

    def remove_field(self, field_type):
        self.fields = [f for f in self.fields if not isinstance(f, field_type)]

    def edit_field(self, field_type, new_value):
        for field in self.fields:
            if isinstance(field, field_type):
                field.value = new_value
                return

    def find_field(self, field_type):
        for field in self.fields:
            if isinstance(field, field_type):
                return field

    def add_phone(self, phone_number):
        try:
            new_phone = Phone(phone_number)
            self.add_field(new_phone)
        except InvalidPhoneNumberError as e:
            print(f"Error: {e}")

    def remove_phone(self, phone_number):
        self.fields = [f for f in self.fields if not (isinstance(f, Phone) and f.value == phone_number)]

    def edit_phone(self, new_phone_number):
        if Phone.is_valid_phone(new_phone_number):
            phone_field = self.find_field(Phone)
            if phone_field:
                phone_field.value = new_phone_number
            else:
                raise ValueError("Phone number does not exist")
        else:
            raise ValueError("Invalid phone number format")

    def edit_birthday(self, new_birthday):
        birthday_field = self.find_field(Birthday)
        if birthday_field:
            birthday_field.value = new_birthday
        else:
            self.add_field(Birthday(new_birthday))

    def days_to_birthday(self):
        birthday_field = self.find_field(Birthday)
        if birthday_field:
            return birthday_field.days_to_birthday()
        return None

class AddressBook(UserDict):
    def add_record(self, record):
        if isinstance(record, Record):
            self.data[record.name.value] = record

    def remove_record(self, name):
        if name in self.data:
            del self.data[name]

    def search_records(self, criteria):
        results = []
        for record in self.values():
            criteria_matched = False
            for criteria_value in criteria:
                if (
                    str(record.name).lower() == criteria_value.lower()
                    or any(
                        str(field).lower() == criteria_value.lower()
                        for field in record.fields
                    )
                ):
                    criteria_matched = True
                    break

            if criteria_matched:
                results.append(record)

        return results

    def find(self, name):
        return self.data.get(name)

    def delete(self, name):
        if name in self.data:
            del self.data[name]

    def records_iterator(self, page_size=3):
        keys = list(self.data.keys())
        current_page = 0
        while current_page < len(keys):
            start = current_page
            end = current_page + page_size
            page_keys = keys[start:end]
            yield [self.data[key] for key in page_keys]
            current_page += page_size



def input_error(function):
    """
    A decorator to handle errors that may occur due to user input.
    :param function: User input function.
    :return: Either the operation of the function or the text with an error, to be re-entered.
    """
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        # except KeyError:
        #     return 'Wrong name'
        except ValueError:
            return None  # Return None instead of the error message
        except IndexError:
            return 'Please provide the required input'
        # except TypeError:
        #     return 'Wrong command.'
    return wrapper

@input_error
def hello_func():
    return "How can I help you?"

@input_error
def exit_func():
    return "Goodbye! Have a nice day!"

@input_error
def add_func(data, name, phone, birthday=None):
    # Перевіряємо, чи введений номер телефону є числовим і має принаймні 10 цифр
    if not Phone.is_valid_phone(phone):
        print("Invalid phone number format. Phone must be a numeric value with at least 10 digits.")
        return None

    record = Record(name)
    if birthday:
        record.add_field(Birthday(birthday))
    record.add_phone(phone)
    data.add_record(record)
    return f"Added {name} with phone number {phone} and birthday {birthday}."



@input_error
def change_func(data, name, phone, birthday=None):
    contact = data.find(name.lower())

    if contact is not None:
        contact.edit_phone(phone)
        # Якщо користувач ввів нову дату, то оновлюємо її
        if birthday is not None:
            contact.edit_field(Birthday, birthday)

        # Оновлюємо дані в адресній книзі
        data.add_record(contact)  # Update the contact in the address book

        return f"Contact information updated for {name}."
    else:
        # Return an empty Record object when the contact is not found.
        # This ensures that the function always returns a Record object.
        return Record("")




@input_error
def show_func(data):
    page_size = 3
    page_num = 1

    while True:
        start_index = (page_num - 1) * page_size
        end_index = start_index + page_size
        current_page = list(data.values())[start_index:end_index]

        if not current_page:
            print("No more contacts to show.")
            break

        print(f"Page {page_num}/{len(data) // page_size + 1}:")
        for contact in current_page:
            print(f"Name: {contact.name.value}")
            phone_field = contact.find_field(Phone)
            if phone_field:
                print(f"Phone: {phone_field.value}")
            else:
                print("Phone: (not set)")
            birthday_field = contact.find_field(Birthday)
            if birthday_field:
                print(f"Birthday: {birthday_field.value}")
            else:
                print("Birthday: (not set)")

        user_input = input("Enter 'next' to see the next page, 'prev' to see the previous page, or 'exit' to exit: ").strip()
        if user_input.lower() == 'next':
            page_num += 1
        elif user_input.lower() == 'prev':
            page_num = max(1, page_num - 1)
        elif user_input.lower() == 'exit':
            break
        else:
             print("Invalid command. Please enter 'next', 'prev', or 'exit'.")

 

@input_error
def phone_func(data, name):
    contact = data.find(name.lower())
    if contact:
        phone_field = contact.find_field(Phone)
        if phone_field:
            return f"Phone number for {name} is {phone_field.value}"
        else:
            return "Phone number not found for this contact."
    else:
        return "Contact not found."

def handle_hello(user_input):
    print(hello_func())

def handle_exit(user_input):
    print(exit_func())
    return True  # Signal to exit the loop

def handle_add(contacts_dict):
    while True:
        name = input("Enter name: ").strip()
        if not name:
            print("Name cannot be empty.")
            continue  # Повторити введення імені, якщо воно порожнє

        if any(char.isdigit() for char in name):
            print("Name cannot contain numeric characters.")
            continue  # Повторити введення імені, якщо воно містить цифри
            
        phone = input("Enter phone: ").strip()
        if not phone.strip().replace("+", "").isdigit():
            print("Phone must be a numeric value, not a string")
            continue  # Continue the loop if phone contains letters

        birthday_input = input("Enter birthday (YYYY-MM-DD) or leave blank: ").strip()
        
        if birthday_input:
            # Validate the date format
            try:
                datetime.strptime(birthday_input, "%Y-%m-%d")
            except ValueError:
                print("Invalid date format. Please use YYYY-MM-DD.")
                continue

        try:
            # Add a contact with or without a birthday
            add_result = add_func(contacts_dict, name, phone, birthday_input)
            if add_result:
                print(add_result)
            break  # Exit the loop if successful
        except ValueError:
            pass  # Suppress the error message and return None


def handle_change(contacts_dict):
    name = input("Enter name: ").strip()
    contact = contacts_dict.find(name)
    
    if contact:
        new_birthday = input("Enter new birthday (YYYY-MM-DD) or leave blank: ").strip()
        new_phone = input("Enter new phone: ").strip()
        
        if new_birthday:
            contact.edit_birthday(new_birthday)
        
        if new_phone:
            contact.edit_phone(new_phone)
        
        print(f"Contact information updated for {name}.")
    else:
        print("Contact not found.")




def handle_show(contacts_dict):
    show_result = show_func(contacts_dict)
    if show_result:
        print(show_result)


def handle_phone(contacts_dict):
    name = input("Enter name: ").strip()
    phone_result = phone_func(contacts_dict, name)
    if phone_result:
        print(phone_result)

@input_error
def handle_birthday(contacts_dict):
    name = input("Enter name: ").strip()
    contact = contacts_dict.find(name)
    
     # Search for the contact in a case-insensitive manner
    for key in contacts_dict.data.keys():
        if key.lower() == name:
            contact = contacts_dict.data[key]
            break

    if contact:
        birthday_field = contact.find_field(Birthday)
        if birthday_field:
            days_until_birthday = birthday_field.days_to_birthday()
            print(f"Days until {name}'s birthday: {days_until_birthday} days")
        else:
            print(f"No birthday information found for {name}.")
    else:
        print("Contact not found.")


def handle_search(contacts_dict):
    search_term = input("Enter a name or phone number to search: ").strip()
    search_results = search_contacts(contacts_dict, search_term)
    
    if search_results:
        print("Search Results:")
        for contact in search_results:
            print(f"Name: {contact.name.value}")
            phone_field = contact.find_field(Phone)
            if phone_field:
                print(f"Phone: {phone_field.value}")
            else:
                print("Phone: (not set)")
            birthday_field = contact.find_field(Birthday)
            if birthday_field:
                print(f"Birthday: {birthday_field.value}")
            else:
                print("Birthday: (not set)")
    else:
        print("No matching contacts found.")

# def handle_save_contacts(contacts_dict):
#     save_contacts(contacts_dict)
#     return("Contacts saved")  # Customize the message as desired

def handle_load_contacts(contacts_dict):
    load_contacts(contacts_dict, file_path)
    print("Contacts loaded")

def main():
    # Specify the file path here
    contacts_dict = AddressBook()

    # Load contacts from the pickle file if it exists
    load_contacts(contacts_dict)


    print("Hi!")

    command_handlers = {
        'hello': handle_hello,
        'hi': handle_hello,
        'exit': handle_exit,
        'add': handle_add,
        'change': handle_change,
        'show all': handle_show,
        'phone': handle_phone,
        'birthday': handle_birthday,
        'save contacts': lambda _: save_contacts(contacts_dict),
        'load contacts': lambda: load_contacts(contacts_dict, file_path),
        'search': handle_search,
    }


    while True:
        user_input = input('Enter command for bot: ')
        handler = command_handlers.get(user_input)
        if handler:
            if handler(contacts_dict):
                break
           
        else:
            print("Invalid command. How can I help you?")

if __name__ == "__main__":
    main()

