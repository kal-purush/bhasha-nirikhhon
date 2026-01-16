import random

letters=['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
numbers=['0','1','3','4','5','6','7','8','9']
symbols=['!','#','$','%','^','&','*','+']

print("WELCOME TO MYPAS GENERATOR..!!")

m_letters= int(input("HOW MANY LETTERS SHOULD BE IN YOUR PASSWORD ..??\n"))
m_numbers=int(input("HOW MANY NUMBERS SHOULD BE IN YOUR PASSWORD..?\n"))
m_symbols=int(input("HOW MANY SYMBOLS SHOULD BE IN YOUR PASSWORD.../\n"))

password_list=[]
for char in range(1,m_letters+1):
    password_list.append(random.choice(letters))

for char in range(1,m_numbers+1):
     password_list+=random.choice(numbers)

for char in range(1,m_symbols+1):
    password_list += random.choice(symbols)

# print (password_list)

print(password_list)
random.shuffle(password_list)
# print(password_list)

password=""
for char in password_list:
    password+=char
print(f"YOUR RECOMMENDED PASSWORD IS:{password}")