from __future__ import annotations
import os
from typing import List
from dataclasses import dataclass

os.environ["DAY"] = "04"


class Card:
    def __init__(self, raw_data: str) -> None:
        self._total_score: int = 0
        self._cards_won: int = -1
        card, values = raw_data.split(': ')
        _, number = card.split()
        self.number = int(number)
        win_num, play_num = values.split('|')
        self.winning_numbers = win_num.split()
        self.player_numbers = play_num.split()

    @property
    def cards_won(self) -> int:
        if self._cards_won == -1:
            self._cards_won = 0
            for play_num in self.player_numbers:
                if play_num in self.winning_numbers:
                    self._cards_won += 1

        return self._cards_won

    def total_score(self, family=None) -> int:
        if self._total_score:
            return self._total_score
        total: int = 1
        if self.cards_won:
            if family:
                children = family[self.number:self.number+self.cards_won]
                generation = 1
                child: Card
                for child in children:
                    total += child.total_score(family=family)
                    generation += 1
        self._total_score = total
        return total


def get_card_point_value(input_data: List) -> int:
    list_of_cards: List[Card] = []
    total_card_value: int = 0
    for line in input_data:
        list_of_cards.append(Card(line))
    parent_card: Card = list_of_cards[0]
    for card in list_of_cards:
        total_card_value += card.total_score(list_of_cards)

    return total_card_value