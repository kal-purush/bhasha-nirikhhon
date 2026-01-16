def get_hint(secret, guess):
    
    n = len(secret)

    if n==1 and secret!=guess:
        return '0A0B'
    elif n==1 and secret==guess:
        return '1A0B'

    sdict = {}
    gdict = {}
    x = 0
    c = 0

    for i in range(n):
        sdict[secret[i]] = sdict.get(secret[i], 0) + 1
        gdict[guess[i]] = gdict.get(guess[i], 0) + 1
        if secret[i] == guess[i]:
            x += 1
    
    for k in gdict.keys(): # Counting cows
        if k in sdict:
            c += min(sdict[k], gdict[k])
            

    return str(x) + 'A' + str(c-x) + 'B'
            
get_hint("1807", "7810")
get_hint("1123", "0111")
get_hint("1", "0")
get_hint("1", "1")

get_hint("1211111", "1111112")
get_hint("1122", "1222")