def SprawdzeniePlci(pesel):
    plec = pesel[9]
    if plec % 2 == 0:
        return 'K'
    else:
        return 'M'

def Sumowanie(pesel):
    waga = [1,3,7,9,1,3,7,9,1,3]
    S = sum(pesel[i] * waga[i] for i in range(10))

    M = S % 10
    if M == 0:
        R = 0
    else:
        R = 10-M

    if R == pesel[10]:
        return True
    else:
        return False

SprawdzeniePlci([5,5,0,3,0,1,0,1,1,9,3])
Sumowanie([5,5,0,3,0,1,0,1,1,9,3])

