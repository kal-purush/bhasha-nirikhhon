import random 


letters = ["a","b","c","d","e","f","g","h","i","j","k","l","m",
           "n","o","p","q","r","s","t","u","v","w","x","y","z"]

numbers = ['0','1','2','3','4','5','6','7','8','9']

symbols = ['!','#','$','%','%','&','(',')','*','+']

print("Password Generator!")

nr_letters = int(input(f"How many letters do you want in your password?\n"))
nr_numbers = int(input(f"How many numbers do you want in your password?\n"))
nr_symbols = int(input(f"How many symbols do you want in you password?\n "))

# let the password be empty 

password = ""

# Generate the random letters using a for loop 

for _ in range(nr_letters):
  password+=random.choice(letters)
  
# Generate the number numbers. 
  
  for _ in range(nr_numbers):
    password+=random.choice(numbers)
    
#Generate the symbol 

for _ in range(nr_symbols):
  password+= random.choice(symbols)
    
print(password)

# shuffle the password.

password_list = list(password)
random.shuffle(password_list)

final_password = ''.join("password_list")

    
