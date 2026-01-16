# my 1st .exe file
def my_game():
    import random as rd
    print("stone,paper,scissor game ::::: play for fun")
    print(" s->x win\n", "x->p win\n", "p->s wim\n", "other wise you lose! :-)\n")
    lst = ["s", "p", "x"]
    print(" s = stone:\n", "p = paper:\n", "x = scissor:\n")
    count = 10
    mpoint = 0
    ypoint = 0
    print("You have 10 chances")
    print(f"your point:{ypoint} and computer point:{mpoint}")
    while count > 0:
        in_put = input("your choice: ")
        choice = rd.choice(lst)
        if in_put == "x" and choice == "p":
            print("you win , your have the point")
            ypoint += 1
        elif in_put == "p" and choice == "s":
            print("you win , your have the point")
            ypoint += 1
        elif in_put == "s" and choice == "x":
            print("you win , your have the point")
            ypoint += 1
        elif in_put == choice:
            print("The input is same as computer")
        else:
            print("you lose!")
            mpoint += 1
        print(choice)
        print(f"your point:{ypoint} and computer point:{mpoint}")
        count = count - 1
        print(f"you have left {count} chances")
    print("GAME OVER....///")
    if mpoint > ypoint:
        print("Computer Wins")
    elif mpoint < ypoint:
        print("You wins")
    elif mpoint == ypoint:
        print("This is a tie..... Try again :-)")


my_game()
print(input("press Enter to exit"))