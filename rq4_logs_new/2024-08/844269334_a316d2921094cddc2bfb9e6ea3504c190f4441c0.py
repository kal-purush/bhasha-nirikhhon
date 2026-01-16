import json
import re

# Initialize the contact dictionary
contacts = {}

def add_contact():
    identifier = input("Enter a unique identifier (phone number or email): ")
    name = input("Enter the contact's name: ")
    phone = input("Enter the phone number: ")
    email = input("Enter the email address: ")
    # Additional info (address, notes) can be added similarly
    contacts[identifier] = {'name': name, 'phone': phone, 'email': email}

def edit_contact():
    identifier = input("Enter the unique identifier of the contact to edit: ")
    if identifier in contacts:
        contacts[identifier]['name'] = input("Enter the updated name: ")
        contacts[identifier]['phone'] = input("Enter the updated phone number: ")
        contacts[identifier]['email'] = input("Enter the updated email address: ")
        print("Contact updated successfully!")
    else:
        print("Contact not found.")

def delete_contact():
    identifier = input("Enter the unique identifier of the contact to delete: ")
    if identifier in contacts:
        del contacts[identifier]
        print("Contact deleted successfully!")
    else:
        print("Contact not found.")

def search_contact():
    identifier = input("Enter the unique identifier to search for: ")
    if identifier in contacts:
        details = contacts[identifier]
        print(f"Contact details for {identifier}:")
        print(f"Name: {details['name']}")
        print(f"Phone: {details['phone']}")
        print(f"Email: {details['email']}")
    else:
        print("Contact not found.")

def display_contacts():
    if not contacts:
        print("No contacts found.")
    else:
        print("All contacts:")
        for identifier, details in contacts.items():
            print(f"{identifier}: {details['name']} ({details['phone']}, {details['email']})")

def export_contacts():
    with open("contacts.json", "w") as file:
        json.dump(contacts, file)
    print("Contacts exported to 'contacts.json'.")

def import_contacts():
    try:
        with open("contacts.json", "r") as file:
            imported_contacts = json.load(file)
            contacts.update(imported_contacts)
        print("Contacts imported successfully.")
    except FileNotFoundError:
        print("No existing contacts file found.")

def main():
    while True:
        print("\nMenu:")
        print("1. Add a new contact")
        print("2. Edit an existing contact")
        print("3. Delete a contact")
        print("4. Search for a contact")
        print("5. Display all contacts")
        print("6. Export contacts")
        print("7. Import contacts")
        print("8. Quit")
        choice = input("Enter your choice: ")

        if choice == '1':
            add_contact()
        elif choice == '2':
            edit_contact()
        elif choice == '3':
            delete_contact()
        elif choice == '4':
            search_contact()
        elif choice == '5':
            display_contacts()
        elif choice == '6':
            export_contacts()
        elif choice == '7':
            import_contacts()
        elif choice == '8':
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()