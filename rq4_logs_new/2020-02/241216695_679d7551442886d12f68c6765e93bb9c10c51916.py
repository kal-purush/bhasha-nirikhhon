# GuessNum - a simple python console game where the player has to guess a random number
# Coyright Roger Smith February 2020

class GuessNum:
    def __init__(self, **kwargs):
        self._beginRange = kwargs['beginRange'] if 'beginRange' in kwargs else 0
        self._endRange = kwargs['endRange'] if 'endRange' in kwargs else 100
        self._value = 0
        self._tries = 0

        # find random value
        from random import randint
        self._value = randint(self._beginRange, self._endRange)

    def beginRange(self, x = None):
        if x: self._beginRangNone = x
        return self._beginRange

    def endRange(self, y = None):
        if y: self._endRange = y
        return self._endRange

    def value(self):
        return self._value

    def tries(self):
        return self._tries

    def guess(self, z):
        self._tries +=1
        if z < self._value: return 'greater'
        elif z > self._value: return 'less'
        else: return 'yep'


def main():
        
    # initiate the GuessNum object
    start = 0
    end = 100
    number = 0
    notWon = True
    g0 = GuessNum(beginRange = start, endRange = end)
      
    print("Hello there.  Let\'s play a game.")
    print("I\'m thinking of a number between {} and {}".format(g0.beginRange(), g0.endRange()))

    print("The number is {}".format(g0.value()))
        

    while notWon:
        number = int(input("Guess a number:"))
        result = g0.guess(number)

        if result == "less": print("The number I\'m thinking of is less than {}".format(number))
        elif result == "greater": print("The number I\'m thinking of is greater than {}".format(number))
        else:
            notWon = False
            print("You got it in {} tries.  Woohoo!".format(g0.tries()))

    print("Good job!  Come back and play again.")
    
if __name__ == '__main__': main()
