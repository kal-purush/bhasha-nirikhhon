"""Implement the sort-cards kata with a priority queue."""


class Priorityq:
    """Define a priority queue and its methods."""

    def __init__(self):
        """Init a pque as a list."""
        self.prq = []

    def insert(self, val, pri=0):
        """Add a value to our priority queue that has an optional priority."""
        if pri < 0:
            raise ValueError("Minimum priority is 0")
        if type(pri) not in (int, float):
            raise TypeError("Priority must be an integer or float")
        self.prq.append([val, pri])
        pos = len(self.prq) - 1
        if len(self.prq) > 1 and pri > 0:
            if pos % 2 == 1:
                parent = pos // 2
            else:
                parent = (pos // 2) - 1
            while self.prq[parent][1] < self.prq[pos][1]:
                tmp = self.prq[parent]
                self.prq[parent] = self.prq[pos]
                self.prq[pos] = tmp
                pos = parent
                if pos == 0:
                    break
                if pos % 2 == 1:
                    parent = pos // 2
                else:
                    parent = (pos // 2) - 1

    def pop(self):
        """Remove the value that is at the top of the prq."""
        if not self.prq:
            return None
        top = self.prq[0][0]
        self.prq[0] = self.prq[-1]
        del self.prq[-1]
        pos = 0
        if len(self.prq) > 1:
            left = (pos * 2) + 1
            right = (pos * 2) + 2
            if left > len(self.prq) - 1:
                left = pos
            if right > len(self.prq) - 1:
                right = pos
            while self.prq[left][1] > self.prq[pos][1] or self.prq[right][1] > self.prq[pos][1]:
                if self.prq[left][1] > self.prq[right][1]:
                    switch_with = left
                else:
                    switch_with = right
                tmp = self.prq[switch_with]
                self.prq[switch_with] = self.prq[pos]
                self.prq[pos] = tmp
                pos = switch_with
                left = (pos * 2) + 1
                right = (pos * 2) + 2
                if left > len(self.prq) - 1:
                    left = pos
                if right > len(self.prq) - 1:
                    right = pos
        return top


def sort_cards(cards):
    """Sort shuffled list of cards, sorted by rank."""
    store = Priorityq()
    for card in cards:
        if card == 'T':
            pri = 10
        elif card == 'J':
            pri = 11
        elif card == 'A':
            pri = 1
        elif card == 'Q':
            pri = 12
        elif card == 'K':
            pri = 13
        else:
            pri = int(card)
        store.insert(card, pri)
    new_cards = []
    for card in cards:
        new_cards.insert(0, store.pop())
    return new_cards