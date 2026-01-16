MAX_OUTCOME = 500
MAX_WITHDRAWS = 3
balance = 0
extract = ""
withdraws = 0
withdraw_value = 0
income_value = 0

menu = """
[1] Depósito
[2] Saque
[3] Extrato
[4] Sair
"""

while True:
    option = input(menu)

    if option == "1":
        income_value = float(input("Valor do depósito: "))

        if income_value > 0:
            balance += income_value
            extract += f"Depósito: R$ {income_value:.2f}\n"

        else:
            print("Valor inválido.")

    elif option == "2":
        withdraw_value = float(input("Valor do saque: "))

        if balance - withdraw_value < 0:
            print("Saldo insuficiente")

        elif withdraws == MAX_WITHDRAWS:
            print("Limite de saques diário atingido.")

        elif withdraw_value > MAX_OUTCOME:
            print("Valor acima do limite de saque.")

        elif withdraw_value > 0:
            withdraws += 1
            balance -= withdraw_value
            extract += f"Saque: R${withdraw_value:.2f}\n"

        else:
            print("Valor inválido.")

    elif option == "3":
        print("Sem movimentações.") if not extract else extract
        print(f"\nSaldo: R${balance:.2f}")

    elif option == "4":
        break

    else:
        print("Operação inválida.")