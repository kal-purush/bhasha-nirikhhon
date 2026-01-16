# montecarlo simulation of yotta lottery savings account

import random
import pdb
import sys
import time

# generate a ticket of 1 yotta number (integer 1-25) and six other distinct
# integers 1-70; arranged as list w/ yotta number first
# could make future operations more efficient by sorting or using sets, but
# probably not worth the trouble
def get_ticket():
    ticket = [random.randint(1, 25)]
    while len(ticket) < 7:
        next = random.randint(1, 70)
        if next not in ticket[1:]:
            ticket.append(next)
    return ticket

# same thing but set based to maybe improve peformance
# tests indicate it doesn't really make a difference at this scale (small tix)
def get_ticket2():
    yotta = random.randint(1, 25)
    rest = set()
    while len(rest) < 7:
        next = random.randint(1, 70)
        if next not in rest:
            rest.add(next)
    return (yotta, rest)
#get_ticket = get_ticket2

# find the reward associated with a particular ticket
def reward(ticket, jackpot):

    if len(ticket) == 2: # if we're using set tickets...
        nmatches = len(ticket[1] & jackpot[1])
    else: # if we're using traditional tickets...
        nmatches = 0
        for i in ticket[1:]:
            if i in jackpot[1:]:
                nmatches += 1

    # https://www.withyotta.com/official-rules
    if ticket[0] == jackpot[0]: # yotta hit
        if nmatches == 0: return 0.1
        if nmatches == 1: return 0.15
        if nmatches == 2: return 0.75
        if nmatches == 3: return 10
        if nmatches == 4: return 500 # model: split so could be zero in limit
        if nmatches == 5: return 1000 # ditto
        if nmatches == 6: return 5800000 # ditto
    else:
        if nmatches == 0: return 0
        if nmatches == 1: return 0
        if nmatches == 2: return 0
        if nmatches == 3: return 0.4
        if nmatches == 4: return 15
        if nmatches == 5: return 800 # model: split so could be zero in limit
        if nmatches == 6: return 10000 # ditto

# based on an amount of money, buys the correct number of tickets, runs the
# lotto, and returns the new principal
def simulate_week(principal):
    ntickets = int(principal // 25)
    tickets = [get_ticket() for _ in range(ntickets)]
    jackpot = get_ticket()
    rewards = [reward(ticket, jackpot) for ticket in tickets]
    #pdb.set_trace()
    return round(principal + sum(rewards), 2)

# simulates a whole year including 0.2% compounded interest, coarsely
def simulate_year(principal):
    for week in range(52):

        # 0.2% interest compounded monthly
        if week in [4, 8, 12, 17, 21, 25, 30, 34, 38, 43, 47, 51]:
            principal *= 1 + .002/12
            principal = round(principal, 2)

        principal = simulate_week(principal)

    return principal

# runs a simulation for many individuals for a year at some principal
def run_simulation(npeople, principal):
    results = []
    for i in range(npeople):
        sys.stdout.write('\r')
        sys.stdout.flush()
        sys.stdout.write("simulation %d/%d" % (i, npeople))
        results.append(simulate_year(principal))
    results.sort()
    print()

    for i in range(0, 91, 10):
        print("%3d%%: $%.2f" % (i, results[npeople * i // 100]))
    print("100%%: $%.2f" % results[-1]) # dodging a fencepost error

    return results

def main():
    # simulation for Tom ~1 hour @10k,10k
    t0 = time.time()
    run_simulation(10000, 10000)
    t1 = time.time()
    print("runtime: %d seconds" % int(t1 - t0))

main()