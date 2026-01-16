# d = { "key1" : { "key2" : "item2" },
# "kucing" : [3, "jerapah"] };
# print(d["key1"]);
# print(d["key1"]["key2"]);
# print(d["kucing"]);
# print(d["kucing"][1]);

# newList = [ 1, 3, "test1", "test2" , 2, 3, "test1" ];
# s = set(newList);
# print(s);
# print(list(s)[2]);

# listNum = [ 1, 2, 3, 4, 5];
# listNum2 = [item * 2 for item in listNum];
# print(listNum);
# print(listNum2);

# listNum = [ 1, 2, 3, 4, 5];
# listNum = list(map(lambda num: num * 2, listNum));
# print(listNum);

# def genap(num) :
#     return num % 2 == 0;
# listNum = [ 1, 2, 3, 4, 5];
# listNum = list(filter(genap, listNum));
# print(listNum)

# numList = [1,2,3];
# input = 'x';
# check1 = input in numList;
# check2 = 'x' in ['x','y','z'];
# check3 = 'ka' in 'kurakas';
# print(check1);
# print(check2);
# print(check3);

# SOLVES IT!
# def cek(x):
#     x=x.lower()
#     return check in x

# L= ['Merdeka', 'Hello','Hellos','Sohib','Kari ayam']
# print(L)

# check= input('Search : ')
# # print(list(filter(cek,L)))
# print(list(filter(lambda num: check.lower() in num.lower(),L)))


def tampil(Thedictionary):
    print("   Key   |   Value   ")
    print("=====================")
    for x in Thedictionary:
        if (type(Thedictionary[x])==str):
            # print(f"{x}    '{Thedictionary[x]}'")
            print(str(x)+" "*(9-len(x)) +"| '" +str(Thedictionary[x])+"'")
        elif (type(Thedictionary[x])==int):
            # print(f"{x}    '{Thedictionary[x]}'")
            print(str(x)+" "*(9-len(x)) +"|  " +str(Thedictionary[x]))
        else :
            print('Bandel ya itu gak ada di pilihan')
    print("=====================\n\n")

def isiDict(D):
    print("\n\n Membuat Dictionary Baru")
    newkey=input("Masukan key: ")
    tipe=int(input("Tipe Valuenya apa? \n1. Number\n2. String\n Masukan Pilihan : "))
    if (tipe==1):
        newvalue=int(input("Masukan Valuenya : "))
        D[newkey]=newvalue
    elif (tipe==2):
        newvalue=input("Masukan Valuenya : ")
        D[newkey]=newvalue
    else :
        print('Bandel ya itu gak ada di pilihan')


def saring(The):
    tampil(The)
    filteredD={}
    Check=input("input : ")
        # Check=Check.lower()
    print("    Hasil Filter    ")
    for x in The.keys():
        if Check.lower() in x.lower():
            filteredD[x]=The[x]
    tampil(filteredD)


D= {}#{"test": 5, "20" : "9","Maruk":70}

while (True):
    print('Main Menu: \n  1. Lihat Semua Dictionary\n  2. Tambahkan Dictionary\n  3. Filtering isi Dictionary\n  4. Keluar')
    c=int(input('Masukan Pilihan:'))
    if (c==1):
        tampil(D)
    elif(c==2):
        isiDict(D) #tidak perlu di return atau ditampung lagi karena sudah otomatis keupdate penjalsan dibawah ada contoh
    elif(c==3):
        saring(D)
    elif(c==4):
        break


testlist= [1,2,3]
newlist= testlist  #ini tidak mengkopi, "newlist" berarti sebutan lain/alamat baru dari testlist
newlist[2]= "test"
print(testlist)

#untuk menduplikat
newlist = []

newlist=testlist.copy()

newlist[2]=20
print(testlist)
print(newlist)