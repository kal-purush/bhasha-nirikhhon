from cryptography.fernet import Fernet # used to encrypt text

'''
def write_key():
    key = Fernet.generate_key()
    with open('key.key', 'wb') as key_file:
        key_file.write(key)
                            '''
# write_key()

def load_key():
    file = open('key.key', 'rb')
    key = file.read()
    file.close()
    return key


master_pwd = input("What is the master password?")
key = load_key() + master_pwd.encode() # turing the code into bytes
fer = Fernet(key)

def view():
    with open('password.txt', 'r') as f:
        for i in f.readlines():
            data = i.rstrip() # will stop from printing an space after the printing
            (user,passpwd) = data.split('|')
            print(f"UserName : {user} || Password : {str(fer.decrypt(passpwd.encode()).decode())}")

def add():
    name = input("Enter your name!")
    pwd = input("Enter your password!")
    with open('password.txt', 'a') as f:
        f.write(name + " | " + fer.encrypt(pwd.encode()).decode() + "\n") # storing encrypted passwords

while True:
    mode = input("view existing files or add a new file? (view, add) Press q to quit!").lower()
    if mode == 'q':
        break
    if mode == 'view':
        view()
    elif mode == 'add':
        add()
    else:
        print("invalid mode!")
        continue # will re rerun the loop