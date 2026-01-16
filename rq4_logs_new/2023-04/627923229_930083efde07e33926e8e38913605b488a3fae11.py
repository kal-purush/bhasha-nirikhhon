#!/usr/bin/env python

with open('input-day1.txt') as f:
    numbers = f.read().splitlines()

    done1 = False
    done2 = False

    for x in numbers:
        x = int(x)
        for y in numbers:
            y = int(y)

            if not done1 and x + y == 2020:
                print(("part 1", x * y))
                done1 = True

            if not done2:
                for z in numbers:
                    z = int(z)
                    if x + y + z == 2020:
                        print(("part 2", x * y * z))
                        done2 = True
                        break

            if done1 and done2:
                break
        if done1 and done2:
            break
