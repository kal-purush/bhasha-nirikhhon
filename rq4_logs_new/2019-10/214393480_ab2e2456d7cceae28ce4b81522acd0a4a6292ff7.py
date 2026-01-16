# kartkowka 3

# zad 1
lista = [0,10,8,3,5,4,7]

print("liczby nie parzyste w liscie:")
for i in lista:
    if float(i)%2.>0.:
        print(i)

# zad 2
print("Podaj N")
N = int(input())
print()

for i in range(1,N+1):
    print(str("*")*i)

for i in range(N-1,0,-1):
    print(str("*")*i)