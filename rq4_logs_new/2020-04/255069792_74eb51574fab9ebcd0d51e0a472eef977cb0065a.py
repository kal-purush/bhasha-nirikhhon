import math
import sys

cases = int(input())


def SolveACase(case):
    x, y = [int(x) for x in input().split()]

    power = math.ceil(math.log(abs(x) + abs(y), 2))

    xL, yL = [], []

    winners = []

    def bruteforce(n):
        # print(sum(xL), sum(yL))
        if sum(xL) == x and sum(yL) == y:

            local = []
            for el in xL.copy():
                local.append(['x', el])
            for el in yL.copy():
                local.append(['y', el])
            winners.append(local)

            return None

        if n == power + 1:
            return None
        xL.append(2 ** n)
        bruteforce(n + 1)
        xL.pop()

        xL.append(-2 ** n)
        bruteforce(n + 1)
        xL.pop()

        yL.append(2 ** n)
        bruteforce(n + 1)
        yL.pop()

        yL.append(-2 ** n)
        bruteforce(n + 1)
        yL.pop()

    bruteforce(0)

    winners.sort(key=len)

    if len(winners) == 0:
        string = 'IMPOSSIBLE'
    else:
        string = ''
        winners[0].sort(key=lambda i: abs(i[1]))

        for el in winners[0]:

            if el[0] == 'x' and el[1] > 0:
                string += 'E'
            elif el[0] == 'x' and el[1] < 0:
                string += 'W'
            elif el[0] == 'y' and el[1] > 0:
                string += 'N'
            elif el[0] == 'y' and el[1] < 0:
                string += 'S'

    print('Case #' + str(case + 1) + ': ' + string)


for i in range(cases):
    SolveACase(i)