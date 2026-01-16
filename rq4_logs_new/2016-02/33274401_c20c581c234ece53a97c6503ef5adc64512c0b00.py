# Krystal Kwan
# 2/14/2016
# Print a rectangle according to the user's output

while True:
    print "This program will print a rectangle according to your input:"
    print "What is the height?"
    h = int(raw_input("> "))
    print "What is the length?"
    l = int(raw_input("> "))
    for a in range(0,h):
        for b in range(0,l):
            print "*",
        print
    print "Do you want to continue (y/n)?"
    ans = str(raw_input("> "))
    if ans == "y":
        continue
    else:
        break