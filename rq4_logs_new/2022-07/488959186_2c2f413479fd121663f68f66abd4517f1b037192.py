#Napraviti generator funkcije za ispis svih parnih i svih
#neparnih brojeva manjih od prosljeđenog parametra

def parnost(n):
    parna=[]
    neparna=[]
    a=1
    b=0
    for i in range(n):   
        if b<n:
            parna.append(b)  
            b=b+2
        if a<n:
            neparna.append(a)   
            a=a+2
        if n==1 or n==0:
            print(" nema parnih i neparnih brojeva ispod ovog broja")
            yield 
        if b>=n or a>n:
            yield parna
            yield neparna


            

broj=int(input("unesite broj za provjeru parnih i neparnih brojeva: "))
gen=parnost(broj)

print("ovo su parni brojevi: "+str(next(gen)))
print("ovo su neparni brojevi: "+str(next(gen)))

    
    
        
        




    



