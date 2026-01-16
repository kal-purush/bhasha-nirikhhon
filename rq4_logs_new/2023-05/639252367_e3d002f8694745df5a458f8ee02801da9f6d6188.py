# DigiCore Password Manager Script V1.4, written by David Gilmore.  This is a simple script to allow DigiCore staff
# to save their stored password. Script created on 10th May 2023.

# The first part of the script here is for the main menu. Ive used the define function so i can come back to the menu each time
import time

def thinking_animation():
    for i in range(6):
        print(".", end="", flush=True)
        time.sleep(0.5)

def main_menu():
    print("Welcome to the DigiCore Password Manager")
    thinking_animation()
    print("\nPress 1 to save a new password")
    print("Press 2 to view all saved passwords")
    print("Press 3 to quit")

# Now we have our choices. Depending on what option is chosen the a function is created and the user is sent to that part of the code block.
    choice = input("\nEnter your choice: ")
    if choice == "1":
        save_password()
        main_menu()
    elif choice == "2":
        view_passwords()
        main_menu()
    elif choice == "3":
        print("Shutting down")
        exit()

#Using the else command here covers the user inputting anything other then 1,2 or 3 and asks them to try again and returning to the main menu after
    else:
        print("Invalid choice, please select either 1,2 or 3")
        main_menu()

# In this section of the code we handle the creation of the text file using the Append command which makes sure if there is not already a file
# one is created for us
def save_password():
    passwords = open("pass.txt", 'a')
    site = input("What is the URL or resource?: ")
    username = input("What is the Username: ")
    password = input("What is the Password: ")
    passwords.write("\n ====== Password for ")
    passwords.write(site + " ======")
    passwords.write("\n")
    passwords.write("Username: ")
    passwords.write(username)
    passwords.write("\n")
    passwords.write("Password: ")
    passwords.write(password)
    print('\nYou password has been saved. Use option 2 in the menu to retrieve your passwords\n')
    passwords.write("\n")
    passwords.close()

# This next block of code handles the viewing of the text file, which is opened and read in read only format. Once this block has run
# the user is returned to the menu.

def view_passwords():
    # Your code to view the saved passwords goes here
    print("Your stored passwords:")
    file_object = open("pass.txt", "r")
    print(file_object.read())

if __name__ == "__main__":
    main_menu()