import random
# Change1
class Card:
    def __init__(self, rank, suit, weight):
        self.rank = rank
        self.suit = suit
        self.sign = suit + rank
        self.weight = weight

class Deck:
    def __init__(self):
        self.cards = []
        suits = ['spades', 'clubs', 'hearts', 'diamonds']
        ranks = ['2', '3', '4', '5', '6', '7', '8', '9', 'T', 'J', 'Q', 'K', 'A']
        weights = {'2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9, 'T': 10, 'J': 11, 'Q': 12, 'K': 13, 'A': 14}

        for suit in suits:
            for rank in ranks:
                self.cards.append(Card(rank, suit, weights[rank]))

    def shuffle(self):
        random.shuffle(self.cards)

    def take_from_top(self):
        if len(self.cards) > 0:
            return self.cards.pop(0)
        else:
            return None
    
    def take_from_bottom(self):
        if len(self.cards) > 0:
            return self.cards.pop()
        else:
            return None
    
class Player:
    def __init__(self):
        self.hand = []

    def add_to_hand(self, card):
        self.hand.append(card)

    def play_card(self):
        if len(self.hand) > 0:
            return self.hand.pop(0)
        else:
            return None
        
    def collect_cards(self, cards):
        self.hand.extend(cards)

class AI(Player):
    def __init__(self):
        self.hand = []

    def add_to_hand(self, card):
        self.hand.append(card)

    def play_card(self):
        if len(self.hand) > 0:
            return self.hand.pop(0)
        else:
            return None
        
    def collect_cards(self, cards):
        self.hand.extend(cards)
    
def play_war_game():
    deck = Deck()
    deck.shuffle()

    player = Player()
    ai = AI()

    # Distribute all cards between two players
    for _ in range(26):
        player.add_to_hand(deck.take_from_top())
        ai.add_to_hand(deck.take_from_top())

    while len(player.hand) > 0 and len(ai.hand) > 0:
        round_cards = []

        player_card = player.play_card()
        ai_card = ai.play_card()

        round_cards.extend([player_card, ai_card])
        print(f"Cheater: {player_card.sign} vs AI: {ai_card.sign}")

        while player_card.weight == ai_card.weight:
            # Check if players have enough cards for tiebreaker
            if len(player.hand) < 4 or len(ai.hand) < 4:
                return "Game over - not enough cards for a tiebreaker"

            # Place three face-down cards and one face-up card for tiebreaker
            for _ in range(3):
                if player.hand: round_cards.append(player.play_card())
                if ai.hand: round_cards.append(ai.play_card())

            if not player.hand or not ai.hand:
                return "Game over - a player ran out of cards during tiebreaker"

            player_card = player.play_card()
            ai_card = ai.play_card()

            round_cards.extend([player_card, ai_card])
            print(f"Tiebreaker - Cheater: {player_card.sign}, AI: {ai_card.sign}")

        # Determine round winner
        if player_card.weight > ai_card.weight:
            player.collect_cards(round_cards)
        elif ai_card.weight > player_card.weight:
            ai.collect_cards(round_cards)

    # Determine game winner
    if len(player.hand) > len(ai.hand):
        return "Cheater wins!"
    else:
        return "AI wins!"

print(play_war_game())