"""
THE FOLLOWING CODE IS A LITTLE WORDLE PROGRAM
A SIMPLE GAME MADE TO PRACTICE CODE IN PYTHON
"""

# 1°) part consists of importing modules like rich, random and
# the list of words from the file word.py we are going to use
# for the game

from rich.console import Console
from random import choice
from word import word_list
from rich.prompt import Prompt

# 2°) part, we implement the welcome message with a simple print
# The instruction "You may start guessing" is a simple invitation to
# start guessing and the number of allowed guesses are set
# We can add a print to inform the player about the number of allowed guesses

WELCOME_MESSAGE = f'\n[white on blue] WELCOME TO WORDLE [/] \n'
PLAYER_INSTRUCTIONS = "You may start guessing\n"
ALLOWED_GUESSES = 6
NUMBER_GUESSES = f"\n[white on blue] the number of allowed_guesses are {ALLOWED_GUESSES} [/] \n"

# 3°) We invite the player to start the game
GUESS_STATEMENT = "\nEnter your guess"

# 4°) We Set the color of the squares which rely on each result given by the player's action
SQUARES = {'correct_place': '🟩', 'correct_letter': '🟨', 'incorrect_letter': '⬛'}


# 5°) We define the rules of the game with the following functions :
def correct_place(letter):
    return f'[black on green]{letter}[/]'


def correct_letter(letter):
    return f'[black on yellow]{letter}[/]'


def incorrect_letter(letter):
    return f'[black on white]{letter}[/]'


# 6°) We define the function which will check the action 'guess' of the player with the current attended 'answer'
def check_guess(guess, anwser):
    # We set a list of guessed letters
    guessed = []
    # We set a list for the wordle pattern
    wordle_pattern = []
    # we set a loop to retrieve each value of letter in the guess action taken by the player with the enumerate method
    for i, letter in enumerate(guess):
        # We define a set of conditions in the loop for each 'answer param' and guess param to ensure its correct place
        # We increment the guessed letter accordingly with its correct place, and we append the wordle pattern list
        # Followed by the condition 'elif the letter already exists' in the attended answer
        # We increment the guessed letter accordingly with the correct letter
        # Lastly we set the incorrectness of the action in the else
        if anwser[i] == guess[i]:
            guessed += correct_place(letter)
            wordle_pattern.append(SQUARES['correct_place'])
        elif letter in anwser:
            guessed += correct_letter(letter)
            wordle_pattern.append(SQUARES['correct_letter'])
        elif guess[i] >= anwser[i]:
            wordle_pattern.append(SQUARES['incorrect_letter'])
        else:
            guessed += incorrect_letter(letter)
            wordle_pattern.append(SQUARES['incorrect_letter'])
    # Then, when the loop is over, we return the joined answer of the guessed letters and its resulting wordle pattern
    return ''.join(guessed), ''.join(wordle_pattern)


# We define here the method to process the instructions of the game
def game(console, chosen_word):
    # Here we have a set of variables to start and end the game and lists to retrieve
    # the already_guessed letters, the full wordle pattern and the all words guessed
    # from the instructions processed in the while loop
    end_of_game = False
    already_guessed = []
    full_wordle_pattern = []
    all_words_guessed = []

    while not end_of_game:
        # we change here the guess statement in upper case
        guess = Prompt.ask(GUESS_STATEMENT).upper()
        # We start a while loop with the following conditions:
        while len(guess) != 5 or guess in already_guessed:
            if guess in already_guessed:
                console.print("[red]You've already guessed this word!!\n[/]")
            elif len(guess) > 5:
                console.print("[red]TOO LONG!!! Please enter a 5-letter word!!\n[/]")
            else:
                console.print("[red]TOO SHORT!!! Please enter a 5-letter word!!\n[/]")
                guess = Prompt.ask(GUESS_STATEMENT).upper()
            already_guessed.append(guess)
            guessed, pattern = check_guess(guess, chosen_word)
            all_words_guessed.append(guessed)
            full_wordle_pattern.append(pattern)

            console.print(*all_words_guessed, sep="\n")
            if guess == chosen_word or len(already_guessed) == ALLOWED_GUESSES:
                end_of_game = True
        if len(already_guessed) == ALLOWED_GUESSES and guess != chosen_word:
            console.print(f"\n[red]WORDLE X/{ALLOWED_GUESSES}[/]")
            console.print(f"\n[green]Correct Word: {chosen_word}[/]")
        else:
            console.print(f"\n[green]WORDLE {len(already_guessed)}/{ALLOWED_GUESSES}[/]\n")
        console.print(*full_wordle_pattern, sep="\n")


if __name__ == '__main__':
    console = Console()
    chosen_word = choice(f'{word_list}')
    console.print(PLAYER_INSTRUCTIONS)
    game(console, chosen_word)