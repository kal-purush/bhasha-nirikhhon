import random
import sys

def spinUntilBankrup():
    """Simulates spinning in Wheel of Fortune until bankrupt is rolled.
    Returns the number of rounds (including round in which bankrupt is rolled)
    and the total accumulated."""

    rd = 0
    total = 0

    while True:
        spin = random.randrange(12)
        rd += 1

        if spin == 0:
            return [rd, total]
        else:
            total += 100 * spin

def nGames(n):
    """Simulates plaing n games of Wheel of Fortune.
    Returns the average number of rounds and average total gained."""

    avgRound = 0
    avgTotal = 0

    for i in range(n):
        result = spinUntilBankrup()
        avgRound += result[0]
        avgTotal += result[1]

    return [(avgRound + 0.0) / n, (avgTotal + 0.0) / n]

for line in sys.stdin:
    numOfGames = int(line)
    print(nGames(numOfGames))