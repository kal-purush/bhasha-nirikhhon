import random

my_number = int(input('My Number: '))
guesses = 0

while True:
	computer_guess = random.randrange(10)

	if computer_guess == my_number:
		print(f'Computer Has Guess My Secret Number {my_number}.')
		print(f'Left Guesses {3 - guesses}')
		break
	else:
		print(f'Computer Missed My Number {my_number}, His Number was {computer_guess}, Here We Go Again...')
		computer_guess = random.randrange(10)
		guesses = guesses + 1
		print(f'{3 - guesses} guesses left.')
		if guesses == 3:
			print(f'Computer Misses 3 Times, Game Over.')
			break