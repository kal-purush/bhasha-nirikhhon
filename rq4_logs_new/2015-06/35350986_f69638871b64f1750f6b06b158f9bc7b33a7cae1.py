
'''Create an interactive guessing game.  The program should ask for a users name,
pick a random number between 1-100, give teh user hints if they guess wrong,
and congratulate the user before exiting the program if a user guesses correctly'''


def guessing_game():
    user_name = raw_input("hello.  Welcome to my game.  What should I call you?")
    print user_name