# Import packages
import pickle
import socket
from string import ascii_lowercase

import enchant
from colorama import Fore, init

init(autoreset=True)
d = enchant.Dict("en_US")

# Read config file
SOLUTION_FILE = "solution_words.json"
SETTINGS_FILE = "settings.json"

EMPTY = "—"
HEADERSIZE = 10

# Connect to server
while True:
    IP = input("IP: ")
    PORT = int(input("PORT: "))
    USERNAME = input("USRENAME: ")

    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((IP, PORT))
        print("Successfully connected. Waiting for round to end.")
        break

    except IOError:
        print("Could not connect to server.")


# Class game
class Game:
    def __init__(self, width, height, solution_word):
        self.width = width
        self.board = [[EMPTY for _ in range(self.width)] for _ in range(height)]
        self.solution_word = solution_word

        self.correct_places = set()
        self.incorrect_places = set()
        self.wrong_letter = set()

    def run_game(self):  # Logic of the game
        while True:
            self.print_board()
            self.print_letters()
            self.input_word()

            # Check if won
            if won := self.win_check() is not None:
                if not won:
                    self.print_board()
                    send("incomplete")
                    print(f"{Fore.RED}You lose!\n"*3)

                else:
                    send("found word")
                send(USERNAME)
                self.share()
                print("Waiting for others to finish the wordle.")
                self.print_leaderboard()
                print(f"The word was {self.solution_word}")
                break

    def input_word(self):
        while True:
            # Allow the user to input a word
            user_word = input("Enter a word: ").lower()

            # Check if the word is valid
            if len(user_word) != self.width:
                print(f"{Fore.RED}Incorrect length")
                continue

            if not d.check(user_word):
                print(f"{Fore.RED}Not a real word")
                continue

            if list(user_word) in self.board:
                print(f"{Fore.RED}Word already given")
                continue
            break

        # Put user's guess in board
        for i, word in enumerate(self.board):
            if word[0] == EMPTY:
                self.board[i] = list(user_word)
                break

    def print_board(self):
        # Print the board
        print()
        for word in self.board:
            for i, solution_letter in enumerate(self.solution_word):
                letter = word[i]

                if solution_letter == letter:
                    color_fore = Fore.GREEN

                    self.correct_places.add(letter)
                    if letter in self.incorrect_places:
                        self.incorrect_places.remove(letter)

                elif letter in self.solution_word:
                    color_fore = Fore.YELLOW

                    if letter not in self.correct_places:
                        self.incorrect_places.add(letter)

                else:
                    color_fore = ""
                    if letter != EMPTY:
                        self.wrong_letter.add(letter)

                print(f'{color_fore}{word[i]}', end=" ")
            print()

    def print_letters(self):
        # Print correct, incorrect, and wrong letters
        for letter in ascii_lowercase:
            color = ""
            if letter in self.correct_places:
                color = Fore.GREEN

            elif letter in self.incorrect_places:
                color = Fore.YELLOW

            elif letter in self.wrong_letter:
                color = Fore.RED

            print(f"{color}{letter.upper()}", end=" ")
        print()

    def win_check(self):
        for word in self.board:
            solution_list = list(self.solution_word)

            if word == solution_list:
                self.print_board()
                print(f"{Fore.GREEN}You win!\n"*3)
                return True

        return False if self.board[-1][-1] != EMPTY else None

    def share(self):
        print("Shareable emojis:")
        for word in self.board:
            if word[0] == EMPTY:
                break

            for i, solution_letter in enumerate(self.solution_word):
                letter = word[i]

                if solution_letter == letter:
                    print("\N{Large Green Square}", end="")

                elif letter in self.solution_word:
                    print("\N{Large Yellow Square}", end="")

                else:
                    print("\N{Black Large Square}", end="")
            print()

    @staticmethod
    def print_leaderboard():
        leaderboard = receive()
        leaderboard = dict((sorted(leaderboard.items(), key=lambda item: item[1])))  # Sort the dict

        for place, username in enumerate(leaderboard):
            print(f"#{place + 1}: {username} {round(leaderboard[username], 2)}")


def wait_for_gamestart():
    while True:
        if receive() == "game start":
            break

    dimensions = receive()
    solution_word = receive()

    return dimensions, solution_word


def receive():
    message_header = client_socket.recv(HEADERSIZE)
    message_length = int(message_header.decode('utf-8').strip())
    return pickle.loads(client_socket.recv(message_length))


def send(message):
    message = pickle.dumps(message)
    message = f"{len(message):<{HEADERSIZE}}".encode("utf-8") + message
    client_socket.send(message)


# Functions
def main():
    while True:
        dimensions, solution_word = wait_for_gamestart()

        g = Game(*dimensions, solution_word)
        g.run_game()


if __name__ == "__main__":
    main()