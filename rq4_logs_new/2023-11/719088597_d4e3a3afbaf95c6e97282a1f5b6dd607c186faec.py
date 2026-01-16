# I. Kérjünk be egy szöveget és végezzük el rajta a következő műveleteket, 
# majd minden végeredmény jelenítsünk meg a képernyőn még akkor is, ha ezt a feladat külön nem írja! (konzolos feladat)

# Írj metodust, mely paraméterében kap egy szöveget: 
# 1.Számoljuk meg hány szóköz van a szövegben! 

def szokoz(szoveg:str):
    db = 0
    i = 0
    while i < len(szoveg):
        if szoveg[i]== " ":
            db += 1
        i += 1
    return db

# 2. Írjuk ki a szöveget a képernyőre szóközök nélkül. A továbbiakban ezzel a szöveggel dolgozzunk!

def szokoz_nelkul(szoveg:str):
    i = 0
    uj_szoveg: str = " "
    while i < len(szoveg):
        if not (szoveg[i]== " "):
            uj_szoveg+=szoveg[i]
        i+=1
    return uj_szoveg

# 3.Alakítsuk a szöveget kisbetűssé! A továbbiakkal ezzel a szöveggel dolgozzunk!

def kisbetus(uj_szoveg:str):
    uj_szoveg=uj_szoveg.lower()
    uj_szoveg=uj_szoveg.replace("é","e")
    uj_szoveg=uj_szoveg.replace("í","i")
    uj_szoveg=uj_szoveg.replace("ö","o")
    uj_szoveg=uj_szoveg.replace("ő","ö")
    uj_szoveg=uj_szoveg.replace("ó","o")
    uj_szoveg=uj_szoveg.replace("á","a")
    uj_szoveg=uj_szoveg.replace("ü","u")
    uj_szoveg=uj_szoveg.replace("ú","u")
    uj_szoveg=uj_szoveg.replace("ű","u")

    i = len(uj_szoveg) -1
    while 1 >= 0:
        print(szoveg[i], end= " ")
        i -= 1
