num= int(input("Digite um número:"))
print("Números ímpares de 1 até",num)
for x in range(1, num + 1):
    if x % 2 != 0:
        print(x)