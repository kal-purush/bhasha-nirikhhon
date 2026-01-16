i = 1
s = input()
while s!='#':
    if s == 'HELLO':
        print("Case "+str(i)+": ENGLISH")
    elif s == 'HOLA':
        print("Case "+str(i)+": SPANISH")
    elif s == 'ZDRAVSTVUJTE':
        print("Case "+str(i)+": RUSSIAN")
    elif s == 'HALLO':
        print("Case "+str(i)+": GERMAN")
    elif s == 'BONJOUR':
        print("Case "+str(i)+": FRENCH")
    elif s == 'CIAO':
        print("Case "+str(i)+": ITALIAN")
    else:
        print("Case "+str(i)+": UNKNOWN")
    i+=1
    s = input()