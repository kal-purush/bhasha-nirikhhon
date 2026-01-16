def menu():
    print("Welcome to the game.")
    print("Would you like to:")
    print("Play?")
    print("Exit?")
    gameStart = input("Please choose.")
    if gameStart == "play":
        gameRun()

    elif gameStart == "exit":
    #   gameExit()
        print("exiting")
    elif gameStart != "play" or "exit":
        print("----Invalid command----")
        print("Please enter play or exit.")
        menu()

def gameRun():
    print("Greetings traveler.")
    playerName = input("What is your name?")
    print("I see, good to meet you",playerName)
    
menu()