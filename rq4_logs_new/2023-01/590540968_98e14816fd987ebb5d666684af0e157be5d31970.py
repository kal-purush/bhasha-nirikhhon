import random
Cchoice=["Rock","Paper","Scissor"]
while True:
    print("Rock Paper game")
    youwin,computerwin=0,0
    for i in range(1,6):
        print("round",i,"starts")
        print("select any option")
        print("1,Rock","2.Paper","3.Scissor")
        Yourchoice=int(input())
        if Yourchoice==1:
            print("You select rock")
            Yourchoice="Rock"
        elif Yourchoice==2:
            print("You select paper")
            Yourchoice="Paper"
        elif Yourchoice==3:
            print("You select Scissor")
            Yourchoice="Scissor"
        else:
            print("Invalid")
            break
        Computerchoice=random.choice(Cchoice)
        print("Computer selected:",Computerchoice)

        if (Computerchoice==Yourchoice):
            youwin+=1
            computerwin+=1
            print("This Round is drawn")

        elif ((Computerchoice=="Paper" and Yourchoice=="Scissor") or
            (Computerchoice=="Rock" and Yourchoice=="paper") or
            (Computerchoice=="Scissor" and Yourchoice=="Rock")):
            youwin+=1
            print("You win this round")
        else:
            computerwin+=1
            print("Computer win this round")
    if youwin>computerwin:
        print("You win this game")
        print("Score is: Your score-",youwin,"Computer score-",computerwin)
    elif youwin<computerwin:
        print("Computer win this game")
        print("Score is: Your score-", youwin, "Computer score-", computerwin)
    else:
        print("Match is drawn")
        print("Score is: Your score-", youwin, "Computer score-", computerwin)
    user_choice=input("Want to play again?(y/n)")
    if user_choice=="y":
        continue
    else:
        break