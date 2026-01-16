import random

list_of_words = ["south", "ocean", "water", "canada", "virus"]
max_number_of_guesses = 5

def HangmangameSol2():
    current_number_of_guesses = 0
    current_word = random.choice(list_of_words)
    guessed_letters = []

    for index in range(len(current_word)):
        guessed_letters.append ("_")
    while current_number_of_guesses < max_number_of_guesses:
        print (guessed_letters)
        guess = input ("What is your guess ")
        if guess in current_word:
            for index in range (len(current_word)):
                if current_word[index] == guess:
                     guessed_letters [index] = guess
        else:
            current_number_of_guesses += 1 
            print (f"You have {max_number_of_guesses - current_number_of_guesses} guesses left, try again")
        if "_" not in guessed_letters:
            print ("You win!")
            break
    if current_number_of_guesses == max_number_of_guesses:
        print (f"You lose!, the word was {current_word}")

HangmangameSol2()
'''
words = ['one', 'twenty', 'hundred']

import random

word_index = random.randint(0,len(words)-1) #random.choice
word = words[word_index]

answer = []

for char in word:
    answer += '_'
print(answer, end= '')

guess = input('please take guess: ')
letter_position = word.find(guess,0,len(word)-1)

if letter_position == -1:
    guess = input('guess again: ')
else:
    answer.replace(guess,letter_position,len(word)-1)

print(answer)
'''
            