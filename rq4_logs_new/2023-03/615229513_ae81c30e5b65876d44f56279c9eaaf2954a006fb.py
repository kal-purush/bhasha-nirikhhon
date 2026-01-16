import os

def clear():
    if os.name == 'posix':  # For Linux/Unix/MacOS/BSD
        os.system('clear')
    elif os.name == 'nt':  # For Windows
        os.system('cls')
    