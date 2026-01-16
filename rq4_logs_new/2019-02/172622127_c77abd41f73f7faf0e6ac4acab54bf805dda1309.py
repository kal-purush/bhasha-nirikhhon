products = ["Pikachu P", "Bulbasaur P", "Charmander P", "Squirtle P", "Eevee P"]
price = [10, 15, 20, 25, 30]


print("Pokemon Store - Simple Cash Register")

for x in range(5):
    print((x + 1), ".", products[x], "-", price[x])


print()
select = input("Which Pokemon You Want To Buy? : ")


if select == "1":
    quantity = int(input("How Many? : "))
    total = price[0] * quantity
    print("Total: ", total)
    inputMoney = int(input("Enter money: "))
    if inputMoney >= total:
        print("Congratulation! take your pokemon, and here your change: ", inputMoney - total)
    else:
        print("Not enough money")
        
        
elif select == "2":
    quantity = int(input("How Many? : "))
    total = price[1] * quantity
    print("Total: ", total)
    inputMoney = int(input("Enter money: "))
    if inputMoney >= total:
        print("Congratulation! take your pokemon, and here your change: ", inputMoney - total)
    else:
        print("Not enough money")
        
        
elif select == "3":
    quantity = int(input("How Many? : "))
    total = price[2] * quantity
    print("Total: ", total)
    inputMoney = int(input("Enter money: "))
    if inputMoney >= total:
        print("Congratulation! take your pokemon, and here your change: ", inputMoney - total)
    else:
        print("Not enough money")
        
        
elif select == "4":
    quantity = int(input("How Many? : "))
    total = price[3] * quantity
    print("Total: ", total)
    inputMoney = int(input("Enter money: "))
    if inputMoney >= total:
        print("Congratulation! take your pokemon, and here your change: ", inputMoney - total)
    else:
        print("Not enough money")
        
        
elif select == "5":
    quantity = int(input("How Many? : "))
    total = price[4] * quantity
    print("Total: ", total)
    inputMoney = int(input("Enter money: "))
    if inputMoney >= total:
        print("Congratulation! take your pokemon, and here your change: ", inputMoney - total)
    else:
        print("Not enough money")
        
        
else:
    print("Invalid choice")