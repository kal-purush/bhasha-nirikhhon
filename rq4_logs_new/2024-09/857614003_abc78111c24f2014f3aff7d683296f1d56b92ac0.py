from string import punctuation

def print_header():
    header = """
  ___                                     _    ___                    _            _  _            ___  _              _             
 | _ \ __ _  ___ _____ __ __ ___  _ _  __| |  / __| ___  _ __   _ __ | | ___ __ __(_)| |_  _  _   / __|| |_   ___  __ | |__ ___  _ _ 
 |  _// _` |(_-<(_-<\ V  V // _ \| '_|/ _` | | (__ / _ \| '  \ | '_ \| |/ -_)\ \ /| ||  _|| || | | (__ | ' \ / -_)/ _|| / // -_)| '_|
 |_|  \__,_|/__//__/ \_/\_/ \___/|_|  \__,_|  \___|\___/|_|_|_|| .__/|_|\___|/_\_\|_| \__| \_, |  \___||_||_|\___|\__||_\_\\___||_|  
                                                               |_|                         |__/                                      
    """
    print(header)
    print("="*80)
    print("Welcome to the Password Complexity Checker!")
    print("Follow the guidelines below to create a strong password:")
    print("- Password must be at least 8 characters long.")
    print("- Include both uppercase and lowercase letters.")
    print("- Include numbers and special characters (e.g., @, #, $, etc.).")
    print("To exit the program, type 'E', 'e', 'Exit', or 'exit'.")
    print("="*80)

def check_password_strength(password):
    strength_criteria = [
        len(password) >= 8,  # Length check
        any(char.islower() for char in password),  # Lowercase letter check
        any(char.isupper() for char in password),  # Uppercase letter check
        any(char.isdigit() for char in password),  # Digit check
        any(char in punctuation for char in password)  # Special character check
    ]

    strength = sum(1 for criteria in strength_criteria if criteria)

    if strength == 5:
        return "Strong"
    elif 3 <= strength < 5:
        return "Moderate"
    else:
        return "Weak"

def main():
    print_header()
    
    while True:
        password = input("Enter a password to check its strength (or type 'E', 'e', 'Exit', or 'exit' to quit): ")

        if password.lower() in ['e', 'exit']:
            print("Exiting the program. Goodbye!")
            break

        strength = check_password_strength(password)
        print(f"Password strength: {strength}")
        print("="*80)

if __name__ == "__main__":
    main()