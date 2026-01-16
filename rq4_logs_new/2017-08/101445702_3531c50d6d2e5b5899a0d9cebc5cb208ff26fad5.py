from classes import Deck, Player
RANKS_VALUES = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9, 'J': 10, 'Q': 10, 'K': 10}


def player_turn(player, deck):
    valid_move = False
    while not valid_move:
        print "Your cards are: "
        for i in player.hand.cards:
            print i,

        move = raw_input("\nEnter 'H' if you'd like to hit, 'S' if you'd like to stay. \n")

        if move.lower() == 'h':
            valid_move = True
            player.hand.cards.append(deck.deal_card())
            print "Your cards are: "
            for i in player.hand.cards:
                print i,
            if player.hand.get_value() > 21:
                player.bust = True
                print "\nYou busted!"
            if player.hand.get_value() == 21:
                player.blackjack = True
                print "\nBlackjack!"

        elif move.lower() == 's':
            valid_move = True
            player.stay = True
        else:
            print "Please type 'H' or 'S' \n"


def game_over(p1, p2):
    if p1.bust or p1.stay or p1.blackjack:
        if p2.bust or p2.stay or p2.blackjack:
            return True
    return False


def get_winner(p1, p2):
    winner = None
    print "\nP1 value: %d" % p1.hand.get_value()
    print "P2 value: %d" % p2.hand.get_value()
    if not p1.bust and not p2.bust:
        if p1.hand.get_value() > p2.hand.get_value():
            winner = p1
        elif p2.hand.get_value() > p1.hand.get_value():
            winner = p2
        else:
            winner = 'tie'
    else:
        if p1.bust:
            winner = p2
        elif p2.bust:
            winner = p1
    return winner


def main():
    '''Play game of 2 player blackjack on console.'''

    p1 = Player()
    p2 = Player()
    deck = Deck()
    p1_turn = True

    deck.shuffle()

    p1.hand.cards.append(deck.deal_card())
    p2.hand.cards.append(deck.deal_card())
    p1.hand.cards.append(deck.deal_card())
    p2.hand.cards.append(deck.deal_card())

    while not game_over(p1, p2):

        if p1_turn:
            if not p1.stay and not p1.blackjack and not p1.bust:
                print "\n\nPlayer One's Turn!"
                player_turn(p1, deck)
            p1_turn = False
        else:
            if not p2.stay and not p2.blackjack and not p2.bust:
                print "\n\nPlayer Two's Turn!"
                player_turn(p2, deck)
            p1_turn = True

    winner = get_winner(p1, p2)

    if winner is None:
        print "\nBoth players busted. Tie game!"
    elif winner == p1:
        print "\nPlayer One wins!"
    elif winner == p2:
        print "\nPlayer Two wins!"
    else:
        print "\nIt's a tie!"

    print "Game Over!"

if __name__ == '__main__':
    main()