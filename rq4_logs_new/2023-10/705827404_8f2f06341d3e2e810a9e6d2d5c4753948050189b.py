#Gerar 5 numeros aleatórios e colocar numa tupla
#Mostrar a lista de número criados e indicar o numero maior e o menor


from random import randint

numeros = (randint(0,10), randint(0,10), randint(0,10), randint(0,10), randint(0,10))

ordenado = sorted(numeros)
menor_valor = ordenado[0]
maior_valor = ordenado[-1]

print(f'Os números são {numeros}')

print(f'O número menor é {menor_valor}')

print(f'O número maior é {maior_valor}')