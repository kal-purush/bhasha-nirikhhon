def funcaoSoma():
    print("Qual o numero a ser somado")
    x=input("Responda=")
    return x


nSoma=int(input("Quantas somas voce quer fazer ?"))
nNumeros=int(input("Com quantos numeros esta soma sera feita?"))
for p in range(nSoma):
    valores=0
    print("inicio da soma")
    for i in range(nNumeros):
        valores+=int(funcaoSoma())
    print("valor="+str(valores) )