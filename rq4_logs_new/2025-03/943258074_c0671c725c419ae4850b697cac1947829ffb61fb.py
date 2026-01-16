import hashlib

class User:
    users = []  # Store all registered users
    
    def __init__(self, username, password, role):
        self.username = username
        self.password = self.hash_password(password)
        self.role = role
    
    def hash_password(self, password):
        return hashlib.sha256(password.encode()).hexdigest()
    
    @classmethod
    def sign_up(cls, username, password, role):
        if any(user.username == username for user in cls.users):
            print("Username already taken!")
            return None
        
        if role == "Freelancer":
            name = input("Enter your name: ")
            skills = input("Enter your skills (comma separated): ").split(',')
            experience = input("Enter your experience: ")
            hourly_rate = float(input("Enter your hourly rate: "))
            payment_method = input("Enter your payment method: ")
            new_user = Freelancer(username, password, name, skills, experience, hourly_rate, payment_method)
        elif role == "Employer":
            company_name = input("Enter your company name: ")
            new_user = Employer(username, password, company_name)
        else:
            print("Invalid role!")
            return None
        
        cls.users.append(new_user)
        print("Sign-up successful! You can now log in.")
        return new_user
    
    @classmethod
    def login(cls, username, password):
        hashed_pw = hashlib.sha256(password.encode()).hexdigest()
        for user in cls.users:
            if user.username == username and user.password == hashed_pw:
                print(f"Login successful! Welcome, {username}.")
                return user
        print("Invalid username or password!")
        return None
    
    def logout(self):
        print(f"{self.username} has logged out.")
        return None

class Freelancer(User):
    def __init__(self, username, password, name, skills, experience, hourly_rate, payment_method):
        super().__init__(username, password, "Freelancer")
        self.name = name
        self.skills = skills
        self.experience = experience
        self.hourly_rate = hourly_rate
        self.payment_method = payment_method
    
    def view_profile(self):
        print(f"Name: {self.name}\nSkills: {', '.join(self.skills)}\nExperience: {self.experience}\nHourly Rate: ${self.hourly_rate}\nPayment Method: {self.payment_method}")

    def edit_profile(self):
        self.name = input("Enter new name: ")
        self.skills = input("Enter new skills (comma separated): ").split(',')
        self.experience = input("Enter new experience: ")
        self.hourly_rate = float(input("Enter new hourly rate: "))
        self.payment_method = input("Enter new payment method: ")
        print("Profile updated successfully!")

class Employer(User):
    def __init__(self, username, password, company_name):
        super().__init__(username, password, "Employer")
        self.company_name = company_name
    
    def view_profile(self):
        print(f"Company Name: {self.company_name}")

# Testing the User Management System
def main():
    while True:
        print("\n1. Sign Up\n2. Login\n3. Exit")
        choice = input("Select an option: ")
        
        if choice == "1":
            username = input("Enter username: ")
            password = input("Enter password: ")
            role = input("Enter role (Freelancer/Employer): ")
            User.sign_up(username, password, role)
        
        elif choice == "2":
            username = input("Enter username: ")
            password = input("Enter password: ")
            user = User.login(username, password)
            
            if user:
                while True:
                    print("\n1. View Profile\n2. Edit Profile (Freelancer only)\n3. Logout")
                    sub_choice = input("Select an option: ")
                    
                    if sub_choice == "1":
                        user.view_profile()
                    elif sub_choice == "2" and isinstance(user, Freelancer):
                        user.edit_profile()
                    elif sub_choice == "3":
                        user.logout()
                        break
                    else:
                        print("Invalid option!")
        
        elif choice == "3":
            print("Exiting... Goodbye!")
            break
        
        else:
            print("Invalid option! Try again.")

if __name__ == "__main__":
    main()