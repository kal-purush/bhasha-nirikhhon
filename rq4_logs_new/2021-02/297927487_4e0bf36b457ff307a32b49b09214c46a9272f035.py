song = {

    "genre" : "pop",
    "artist" : "maroon5",
    "song name" : "memories"
}


def printSongMetadata():
    print("\n*** Song Metadata ***\n")
    for key in song:
        print(key, ":", song[key])
    print("\n*** End ***\n")


def askQuestion():
    key = input("\nGreat, let\'s start the game, guess the key?\n")
    value = input("\nWhat you think is the value of " + key + "?\n")
    if key and value:
        key = key.lower()
        value = value.lower()
        if key in song and song[key].lower() == value:
            return True
    return False


def startGuessingGame():
    if askQuestion():
        print("Bingo! You guessed it right...")
    else:
        print("Oops... You missed!")
        repeat = input("\nWanna try again? Say 'yes' to continue or say 'no'.\n")
        if repeat.lower() == "yes":
            startGuessingGame()
        else:
            print("\nSee you again!")


# print song metadata
printSongMetadata()

print("Starting game....\n")

# start the guessing game
startGuessingGame()