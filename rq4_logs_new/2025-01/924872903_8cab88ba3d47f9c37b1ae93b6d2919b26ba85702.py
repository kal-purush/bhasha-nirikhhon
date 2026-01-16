import random


def guess():
    value_error = True
    max_int = None
    while value_error:
        try:
            max_int = int(input('What is the maximum possible number you would like? '))
            value_error = False
        except ValueError:
            print("Please input an integer that is greater than 0")
    random_number = random.randint(0, max_int)
    print(f'{random_number}')
    guess_ = None  # Initialize guess_ before the loop
    
    while guess_ != random_number:
        try:
            guess_ = int(input(f'Guess an integer between 0 and {max_int}: '))
        except ValueError:
            print("Please enter a valid integer.")
            continue
        
        if guess_ == random_number:
            print(f'Yay, congrats! You guessed the number {random_number} correctly!!')
        elif guess_ < random_number:
            print('Sorry, guess again. Too low.')
        else:
            print('Sorry, guess again. Too high.')


def computer_guess():
    guess_ = ''
    try:
        high = int(input('Enter the max number for computer guessing: '))
        high += 1
    except ValueError:
        print(f'Max number must be greater than 0')
        quit()
    low = 0
    feedback = ''
    guesses = 0
    while feedback != 'c':
        guesses += 1
        if low != high:
            try:
                guess_ = random.randint(low, high)
            except ValueError:
                print("Input an integer that is greater than 0")
        
        feedback = input(f'Is {guess_} too high (H), too low (L), or correct (C)? ').lower()
        
        if feedback == 'h':
            high = guess_ - 1
        elif feedback == 'l':
            low = guess_ + 1
        elif feedback != 'c':
            print("Invalid input. Please enter 'H', 'L', or 'C'.")
    
    print(f'Yay! The computer guessed your number, {guess_}, correctly, in {guesses} guesses!!')

# Uncomment the function you want to run
guess()

# computer_guess()