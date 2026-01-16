num = float(raw_input("Enter a number: "))

for a in (True, False):
    for b in (True, False):
        print (not (a and b)) == (a or b)