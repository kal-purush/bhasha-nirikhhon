
# Toy project for testing Git hosts 
import Introduction
import NumberGenerator

def congratulations(name):
    print "Congratulations " + name

## Here is a very important comment!
guesses_made = 0
name = raw_input('Hello! What is your name?\n')
Introduction.greetings(name)
lowLimit = NumberGenerator.generateNumber(1,20)
highLimit = NumberGenerator.generateNumber(lowLimit,lowLimit + 20)
number = NumberGenerator.generateNumber(lowLimit, highLimit)
print 'Well, {0}, I am thinking of a number between {1} and {2}.'.format(name,lowLimit, highLimit)

while guesses_made < 6:
    guess = int(raw_input('Take a guess: '))
    guesses_made += 1
    if guess < number:
        print 'Your guess is too low.'
    if guess > number:
        print 'Your guess is too high.'
    if guess == number:
        break
if guess == number:
    congratulations(name)
    print 'You guessed my number in {0} guesses!'.format(guesses_made)
else:
    print 'Nope. The number I was thinking of was {0}'.format(number)
