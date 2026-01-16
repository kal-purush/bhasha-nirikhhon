import random


class BaseballGame():
    def __init__(self, size):
        self.size = size
        # set answer
        answer = []
        for _ in range(self.size):
            random_number = random.randrange(1, 10)
            answer.append(str(random_number))
        self.answer = ''.join(answer)
        self.guess = ""
        self.attempt = 0

    def is_right_input(self, my_answer):
        right_char = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
        if len(my_answer) == self.size:
            for i in range(self.size):
                if my_answer[i] not in right_char:
                    return False
            return True
        else:
            return False

    def set_guess(self):
        while (1):
            try:
                my_answer = input("enter the number!!:")
                if self.is_right_input(my_answer):
                     self.guess=my_answer
                     break
                else:
                    continue
            except:
                print('ektldlqfurgowntpdyksdjskfjksfjkldjfkjsfkld')


    def print_score(self):
        s = 0
        b = 0
        for i in range(len(self.guess)):
            if self.guess[i] == self.answer[i]:
                s += 1
            elif self.guess[i] in self.answer:
                b += 1
        print('{0}S{1}B'.format(s, b))

    def is_over(self):
        if self.answer == self.guess:
            return True
        else:
            return False


    def turn(self):
        self.attempt += 1
        self.set_guess()
        if self.is_over():
            print("NICE GUESS! THE ANSWER IS %s" % self.answer)
            print("YOUR ATTEMPT: %d" % self.attempt)
            return True
        else:
            self.print_score()
            return False


# VARIOUS PLAYERS
playerList = {}
defaultSize = 3
player = None

while True:
    playerName = input("ENTER PLAYER NAME: ")
    if playerName in playerList:
        player = playerList[playerName]
    else:
        print("START NEW GAME")
        playerList[playerName] = BaseballGame(defaultSize)
        player = playerList[playerName]
    is_end = player.turn()
    if is_end:
        playerList.pop(playerName)