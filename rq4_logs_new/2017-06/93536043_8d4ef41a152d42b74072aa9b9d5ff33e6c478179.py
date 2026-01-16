print "Hello, this is fizzbuzz game!"
while True:
    game = int(raw_input("Please enter a number between 1 and 100: "))
    if game > 0 and game < 101:
        break
print game
for game in range(1, (game + 1)):
    if game % 3 == 0 and game % 5 == 0:
        print "fizzbuzz"
    elif game % 3 == 0:
        print "fizz"
    elif game % 5 == 0:
        print "buzz"
    else:
        print game
