"""
Crie um programa que simule o funcionamento de um caixa eletrônico. No início, pergunte ao usuário qual será o valor a ser sacado (número inteiro) e o programa vai informar quantas cédulas de cada valor serão entregues.

OBS: considere que o caixa possui cédulas de R$50, R$20, R$10 e R$1.
"""
# Com o if
value = int(input('Quanto você quer sacar? '))
# Testando com o "INT"
if value / 50 > 0:
    fifty = int(value / 50)
    print(f'Total de cedulas de 50: {fifty}')
    value = value - (fifty * 50)

if int(value / 20) > 0:
    twenty = int(value / 20)
    print(f'Total de cedulas de 20: {twenty}')
    value = value - (twenty * 20)
# Testando com o "//" que é a mesma coisa
if value // 10 > 0:
    ten = value // 10
    print(f'Total de cedulas de 10: {ten}')
    value = value - (ten * 10)

if value // 1 > 0:
    one = value // 1
    print(f'Total de cedulas de 1: {one}')
    value = value - (one * 1)

# Com o while
value = int(input('Quanto você quer sacar? '))
bill = 50
amount = 0

while True:
    if value > bill:
        amount = value // bill
        print(f'Notas de {bill}: {amount}')
        value -= bill * amount
    else:
        if bill == 50:
            bill = 20
        elif bill == 20:
            bill = 10
        elif bill == 10:
            bill = 1
        else:
            break