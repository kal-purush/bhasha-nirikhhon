
menu = """
[d] Depositar
[s] Sacar
[d] Extrato
[d] Sair
"""

saldo = 0
limite = 500
extrato = ""
numero_saques = 0
LIMITE_SAQUES = 3




    
    

while True:

    opcao = input(menu)

    if opcao == "d":
        valor = float(input('Informe o valor para depósito'))

        if valor > 0:
            saldo += valor
            extrato += f"Depósito: R$ {valor:.2f}\n"

        else:
            print('Operação falou! O valor informado é inválido.')

    elif opcao == "s":
        valor = float(input('Informe o valor do saque: '))
        
        excedeu_saldo = valor > saldo
        excedeu_limite = valor > 500
        excedeu_saques = numero_saques >= LIMITE_SAQUES

        if excedeu_saldo:
            print('Não há saldo suficiente')
        elif excedeu_limite:
            print('Falhou! Saque maior que 500. Tente um valor menor.')
        elif excedeu_saques:
            print('Número de saques excedido. Retorne novamente amanhã.')

    elif opcao == "d":
        print("Extrato")
        print(extrato)


    elif opcao == "q":
        break
