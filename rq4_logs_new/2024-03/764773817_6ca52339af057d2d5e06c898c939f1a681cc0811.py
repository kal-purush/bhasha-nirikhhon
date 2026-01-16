import json

def find_best_word(feedback, word_list):
    # Initialize sets and dictionaries to track letter constraints
    not_in_word = set()  # Letters not in the word
    in_word_wrong_position = {}  # Letters in the word but wrong position: letter -> positions
    correct_position = {}  # Letters in the correct position: position -> letter
    
    # Process the feedback to populate the above containers
    for position, word_feedback in enumerate(feedback):
        for pos, (letter, status) in enumerate(word_feedback):
            if status == 1:
                not_in_word.add(letter)
            elif status == 2:
                if letter in in_word_wrong_position:
                    in_word_wrong_position[letter].add(pos)
                else:
                    in_word_wrong_position[letter] = {pos}
            elif status == 3:
                correct_position[pos] = letter

    def word_matches_constraints(word):
        # Check against letters that must not be in the word
        if any(letter in not_in_word for letter in word):
            return False
        # Check for letters that are in the correct position
        for pos, letter in correct_position.items():
            if word[pos] != letter:
                return False
        # Check for letters that must be in the word but in different positions
        for letter, positions in in_word_wrong_position.items():
            if letter not in word:
                return False
            if all(word[pos] == letter for pos in positions):
                return False
        return True

    # Filter the word list based on constraints and return the first match
    for word in word_list:
        if word_matches_constraints(word):
            return word
    return None  # Return None if no suitable word is found

# Define the feedback and word list for the example
feedback = [[[['s', 2], ['o', 2], ['a', 1], ['r', 1], ['e', 1]], [['c', 1], ['l', 2], ['i', 1], ['n', 1], ['t', 1]], [['w', 1], ['h', 3], ['u', 1], ['p', 1], ['s', 3]], [['a', 1], ['b', 1], ['b', 1], ['e', 1], ['d', 2]]], [[['s', 2], ['o', 2], ['a', 1], ['r', 2], ['e', 3]], [['c', 1], ['l', 1], ['i', 1], ['n', 1], ['t', 1]], [['w', 1], ['h', 1], ['u', 1], ['p', 1], ['s', 2]], [['a', 1], ['b', 1], ['b', 1], ['e', 2], ['d', 1]]], [[['s', 1], ['o', 2], ['a', 1], ['r', 1], ['e', 1]], [['c', 1], ['l', 1], ['i', 2], ['n', 1], ['t', 3]], [['w', 1], ['h', 1], ['u', 2], ['p', 1], ['s', 1]], [['a', 1], ['b', 1], ['b', 1], ['e', 1], ['d', 1]]], [[['s', 1], ['o', 1], ['a', 3], ['r', 3], ['e', 2]], [['c', 1], ['l', 1], ['i', 1], ['n', 1], ['t', 1]], [['w', 3], ['h', 1], ['u', 1], ['p', 1], ['s', 1]], [['a', 2], ['b', 1], ['b', 1], ['e', 2], ['d', 1]]], [[['s', 1], ['o', 2], ['a', 1], ['r', 2], ['e', 1]], [['c', 1], ['l', 3], ['i', 1], ['n', 1], ['t', 1]], [['w', 1], ['h', 1], ['u', 3], ['p', 1], ['s', 1]], [['a', 1], ['b', 1], ['b', 1], ['e', 1], ['d', 1]]], [[['s', 3], ['o', 1], ['a', 1], ['r', 2], ['e', 2]], [['c', 1], ['l', 1], ['i', 1], ['n', 1], ['t', 1]], [['w', 1], ['h', 1], ['u', 2], ['p', 1], ['s', 2]], [['a', 1], ['b', 1], ['b', 1], ['e', 3], ['d', 1]]], [[['s', 3], ['o', 1], ['a', 3], ['r', 1], ['e', 1]], [['c', 1], ['l', 1], ['i', 1], ['n', 1], ['t', 2]], [['w', 1], ['h', 2], ['u', 1], ['p', 3], ['s', 2]], [['a', 2], ['b', 1], ['b', 1], ['e', 1], ['d', 1]]], [[['s', 3], ['o', 2], ['a', 2], ['r', 1], ['e', 1]], [['c', 1], ['l', 3], ['i', 1], ['n', 2], ['t', 1]], [['w', 1], ['h', 1], ['u', 1], ['p', 1], ['s', 2]], [['a', 2], ['b', 1], ['b', 1], ['e', 1], ['d', 1]]]]
filepath = "five_letter.json"
with open(filepath, 'r') as file:
    word_list = json.load(file)

for i in range(0, 8):
    best_word = find_best_word(feedback[i], word_list)
    print(f"{best_word} for board {i+1}")