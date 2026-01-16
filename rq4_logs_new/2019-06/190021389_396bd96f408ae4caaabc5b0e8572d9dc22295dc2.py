import random

def main():
    Face = ['Ace', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine', 'Ten', 'Jack', 'Queen', 'King']
    Suit = ['Clubs', 'Diamonds', 'Hearts', 'Spades']
    Cards = []
    temp = ''
    indexF = 0
    indexS = 0

    for i in range(1,6):
        loop = 1

        while loop == 1:
            indexF = random.randint(0,12)
            indexS = random.randint(0,3)
            print(indexF, indexS, "\n")
            temp = Face[indexF] + ' of ' + Suit[indexS]

            if temp not in Cards:
                Cards.append(temp)
                loop = 0
    
    print(Cards)

main()