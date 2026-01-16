import random

def RollD(dice_type, rolls):
    print("Rolling, Good Luck!")
    for i in range(0, rolls):
        d = random.randint(1, dice_type)
        print("---" +str(d)+ "---")
    print()

dice_types = [100, 20, 12, 8, 4]
exit_option = len(dice_types) + 1
choice = 0

while choice != exit_option:
    for i in range(len(dice_types)):
        print(str(i + 1) + ". d" + str(dice_types[i]))
    
    print(str(exit_option) + ". exit")
    print()

    choice = int(input("What Die Do You Need?: "))

    if(choice == exit_option):
        print("Bye-bye!")
    else:
        rolls = int(input("How many do you need to roll? "))
        RollD(dice_types[choice-1],rolls)