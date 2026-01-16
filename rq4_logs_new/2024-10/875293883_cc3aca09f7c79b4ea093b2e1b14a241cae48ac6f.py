import random

# A list of possible words for the game.
words = ['apple', 'art', 'coding', 'hello', 'world', 'love', 'happy', 'smile', 'peach', 'peace']

# Function to select a random word from the list.
def get_random_word(words):
    return random.choice(words)

# Function to display the current progress of the word.
def display_word(word, guessed_letters):
    # Create a string where correctly guessed letters are shown, and the rest are underscores.
    return ' '.join([letter if letter in guessed_letters else '_' for letter in word])

# Function to play the hangman game.
def play_hangman():
    word = get_random_word(words)  # Get a random word.
    guessed_letters = []  # List to store the letters guessed by the player.
    attempts = 10  # Number of incorrect guesses allowed.

    print("Welcome to Hangman!")
    print(display_word(word, guessed_letters))

    # Loop until the player either wins or runs out of attempts.
    while attempts > 0:
        guess = input("Guess a letter: ").lower()  # Get a guess from the player and make it lowercase.

        if len(guess) != 1 or not guess.isalpha():
            print("Invalid input. Please guess a single letter.")
            continue

        # Check if the letter was already guessed.
        if guess in guessed_letters:
            print("You already guessed that letter.")
        elif guess in word:
            guessed_letters.append(guess)
            print("Correct guess!")
        else:
            guessed_letters.append(guess)
            attempts -= 1  # Decrease the attempts if the guess is incorrect.
            print(f"Incorrect guess! You have {attempts} attempts left.")

        # Display the current progress.
        print(display_word(word, guessed_letters))

        # Check if the player has guessed all letters in the word.
        if all(letter in guessed_letters for letter in word):
            print("Congratulations! You guessed the word!")
            break
    else:
        print(f"Sorry, you ran out of attempts. The word was '{word}'.")

# Call the function to start the game.
play_hangman()