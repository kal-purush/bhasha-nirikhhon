#!/usr/bin/env python

from itertools import cycle
import numpy as np


if __name__ == "__main__":
    n_players = int(input('[ NUMBER OF PLAYERS ] '))

    players = [input(f'\tplayer {i}: ') for i in range(n_players)]
    np.random.randint(n_players)
    max_cards = int(np.ceil(50 / n_players))
    rounds = list(range(max_cards, 0, -1))
    rounds += reversed(rounds[:-1])

    print(f"[ PLAYERS ] {players}")
    print(f"[ ROUNDS ] {rounds}")
    
    bids, trumps, wins, bids_won = {}, {}, {}, {player : 0 for player in players}
    for (round, n_cards), dealer in zip(enumerate(rounds), cycle(players)):
        bids[round] = {}
        n_bid = 0
        trumps[round] = input(f'[ ROUND {round} ] {dealer} deals {n_cards} cards. Enter trump suit to continue: ')
        bid_order = players[round+1:] + players[:round]
        print(f"[ BID ORDER ] {bid_order + [dealer]}")
        for player in bid_order:
            bids[round][player] = int(input(f"\t{player}'s bid ({n_bid} of {n_cards} has been bid): "))
            n_bid += bids[round][player]
        deal_str = f"{n_bid} of {n_cards} has been bid; "
        deal_str += "overbid: dealer can bid anything" if n_bid > n_cards else f"dealer cannot bid {n_cards - n_bid}"
        bids[round][dealer] = int(input(f"\t{dealer}'s bid ({deal_str}): "))
        print(f'[ BIDS ] {bids[round]}')
        wins[round] = [input(f"\thand {j} winner: ") for j in range(n_cards)]
        for player in players:
            n_tricks = len([x for x in wins[round] if x == player])
            print(f"\t{player} bid {bids[round][player]} with {n_tricks} tricks")
            bids_won[player] += (5 + n_tricks) if n_tricks == bids[round][player] else 0
        print(f"[ BIDS WON ] {bids_won}")






