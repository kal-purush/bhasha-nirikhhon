def check_five(self, num):
    result = ''
    if num < 5:
        result = str(num) + " is less than 5."
    elif num == 5:
        result = str(num) + " is equal to 5."
    else:
        result = str(num) + " is greater than 5."
    return result


def who_won(self, player1, player2):
    if player1 == player2:
        return 'TIE!'
    if player1 == 'rock' and player2 == 'paper':
        return 'Player 2 wins!'
    if player1 == 'paper' and player2 == 'scissors':
        return 'Player 2 wins!'
    if player1 == 'scissors' and player2 == 'rock':
        return 'Player 2 wins!'
    return 'Player 1 wins!'