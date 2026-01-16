menu = """

[D] Depositar
[S] Sacar
[E] Extrato
[Q] Sair

"""

saldo = 0
limite = 500
extrato = ""
numero_saques = 0
LIMITE_SAQUES = 3


def addMensagem(valor, operacao):
    return '{} no valor de R$ {:.2f}'.format(
        operacao.upper(), valor) + '\n'


while True:
    opcao = str(input(menu)).upper()

    if opcao == "Q":
        break
    elif opcao == "D":
        deposito = int(input("Digite o valor que deseja depositar: "))
        while deposito <= 0:
            deposito = int(
                input("\nERRO! \n O valor de deposito deve ser maior que 0(zero): "))

        saldo += deposito
        extrato += addMensagem(deposito, "Deposito")
        extrato += addMensagem(saldo, "##Saldo##")
        print(
            f"\n###DEPOSITO###\nSeu deposito de R$ {deposito:.2f} foi realizado com sucesso!")
    elif opcao == "S":
        if LIMITE_SAQUES == numero_saques:
            print(
                f"\nERRO! Você atingiu o limite de {LIMITE_SAQUES} saques diários.\nTente novamente amanhã.")
            continue

        saque = int(input("Digite o valor que deseja sacar: "))
        while saque <= 0:
            saque = int(
                input("\nERRO! \n O valor de saque deve ser maior que 0(zero): "))

        if saque > limite:
            print(
                f"\nERRO! Você não pode sacar mais do que R$ {limite:.2f} em um único saque.\nPor favor tente novamente.")
            continue

        if saque > saldo:
            print(
                f"\nERRO! Você não pode sacar mais do que possui em saldo.\nPor favor tente novamente.")
            continue

        saldo -= saque
        numero_saques += 1
        extrato += addMensagem(saque, "Saque")
        extrato += addMensagem(saldo, "##Saldo##")
        print(
            f"\n###SAQUE###\nSeu saque de R$ {saque:.2f} foi realizado com sucesso!")
    elif opcao == "E":
        print("\n"+extrato)
    else:
        print("ERRO! Opção não encontrada no sistema. Por favor tente novamente")