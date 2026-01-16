helysegek, fajok = [int(x) for x in input().split()]

tomb = []
for i in range(helysegek):
    tomb.append([int(x) for x in input().split()])

talaltunk = False

f = 0
while f < fajok and not talaltunk:
    h = 0
    hanyhelysegben = 0
    # ha legalabb 2 helysegben volt ilyen faj, akkor az mar nem jo
    while h < helysegek and (hanyhelysegben < 2):
        if tomb[h][f] != 0:
            hanyhelysegben += 1
        h += 1
    # ha végigmentünk az oszlopon, és pontosan 1 helységben szerepelt,
    # akkor jó fajt találtunk
    if hanyhelysegben == 1:
        talaltunk = True
    f += 1

if talaltunk:
    print(f) # nem kell az f+1-et kiiratni, mert a while-ciklusban
             # már eleve növeltük eggyel az f értékét.
else:
    print(0)
    