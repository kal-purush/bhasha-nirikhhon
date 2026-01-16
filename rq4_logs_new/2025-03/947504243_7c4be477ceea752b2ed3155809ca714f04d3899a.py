from datetime import datetime, date

# Função para Depósito (Positional-Only Arguments)
def depositar(saldo, valor, extrato, /):
    """
    Realiza um depósito, recebendo argumentos apenas por posição.
    Argumentos: saldo, valor, extrato (positional-only).
    Retorna: saldo atualizado e extrato atualizado.
    """
    try:
        if valor > 0:
            saldo += valor
            data_hora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            extrato += f"{data_hora} - Depósito: R$ {valor:.2f}\n"
            print(f"Depósito de R$ {valor:.2f} realizado com sucesso!")
        else:
            print("Operação falhou! O valor informado é inválido.")
    except ValueError:
        print("Operação falhou! O valor deve ser um número válido.")
    return saldo, extrato

# Função para Saque (Keyword-Only Arguments)
def sacar(*, saldo, valor, extrato, limite, numero_saques, limite_saques, ultimo_saque_data):
    """
    Realiza um saque, recebendo argumentos apenas por nome (keyword-only).
    Argumentos: saldo, valor, extrato, limite, numero_saques, limite_saques, ultimo_saque_data.
    Retorna: saldo, extrato, numero_saques, ultimo_saque_data.
    """
    data_atual = datetime.now().date()
    if ultimo_saque_data != data_atual:
        numero_saques = 0
        ultimo_saque_data = data_atual

    try:
        excedeu_saldo = valor > saldo
        excedeu_limite = valor > limite
        excedeu_saques = numero_saques >= limite_saques

        if excedeu_saldo:
            print("Operação falhou! Você não tem saldo suficiente.")
        elif excedeu_limite:
            print(f"Operação falhou! O valor máximo por saque é R$ {limite:.2f}.")
        elif excedeu_saques:
            print(f"Operação falhou! Limite de {limite_saques} saques diários atingido.")
        elif valor <= 0:
            print("Operação falhou! O valor informado é inválido.")
        else:
            saldo -= valor
            data_hora = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            extrato += f"{data_hora} - Saque: R$ {valor:.2f}\n"
            numero_saques += 1
            print(f"Saque de R$ {valor:.2f} realizado com sucesso!")
    except ValueError:
        print("Operação falhou! Valor inválido.")
    return saldo, extrato, numero_saques, ultimo_saque_data

# Função para Extrato (Positional-Only e Keyword-Only Arguments)
def exibir_extrato(saldo, /, *, extrato):
    """
    Exibe o extrato da conta.
    Argumentos: saldo (positional-only), extrato (keyword-only).
    Não retorna nada (apenas exibe).
    """
    print("\n================ EXTRATO ================")
    print("Não foram realizadas movimentações." if not extrato else extrato)
    print(f"\nSaldo atual: R$ {saldo:.2f}")
    print("==========================================")

# Função para Criar Usuário
def criar_usuario(clientes):
    """
    Cadastra um novo usuário na lista de clientes.
    Argumentos: clientes (lista de dicionários).
    Retorna: None (modifica a lista de clientes diretamente).
    """
    cpf = input("Informe o CPF (somente números): ").strip()
    cpf = ''.join(filter(str.isdigit, cpf))

    if any(cliente['cpf'] == cpf for cliente in clientes):
        print("Erro: Já existe um usuário cadastrado com este CPF!")
        return

    nome = input("Informe o nome completo: ").strip()
    data_nascimento = input("Informe a data de nascimento (dd/mm/aaaa): ").strip()
    endereco = input("Informe o endereço (logradouro, nro - bairro - cidade/UF): ").strip()

    cliente = {
        'nome': nome,
        'data_nascimento': data_nascimento,
        'cpf': cpf,
        'endereco': endereco,
        'contas': []
    }
    clientes.append(cliente)
    print(f"Usuário {nome} cadastrado com sucesso!")

# Função para Criar Conta Corrente
def criar_conta_corrente(clientes, contas):
    """
    Cria uma nova conta corrente e vincula a um usuário.
    Argumentos: clientes (lista), contas (lista).
    Retorna: None (modifica as listas diretamente).
    """
    cpf = input("Informe o CPF do cliente (somente números): ").strip()
    cpf = ''.join(filter(str.isdigit, cpf))

    cliente = next((c for c in clientes if c['cpf'] == cpf), None)
    if not cliente:
        print("Erro: Cliente não encontrado!")
        return

    numero_conta = f"{len(contas) + 1:04d}"
    agencia = "0001"

    conta = {
        'agencia': agencia,
        'numero': numero_conta,
        'cliente_cpf': cpf,
        'saldo': 0,
        'extrato': "",
        'numero_saques': 0,
        'ultimo_saque_data': None
    }
    contas.append(conta)
    cliente['contas'].append(conta)
    print(f"Conta {agencia}-{numero_conta} criada com sucesso para {cliente['nome']}!")

# Nova Função: Listar Clientes
def listar_clientes(clientes):
    """
    Lista todos os clientes cadastrados.
    Argumentos: clientes (lista de dicionários).
    Retorna: None (apenas exibe).
    """
    if not clientes:
        print("Nenhum cliente cadastrado.")
        return

    print("\n================ LISTA DE CLIENTES ================")
    for cliente in clientes:
        print(f"Nome: {cliente['nome']}")
        print(f"CPF: {cliente['cpf']}")
        print(f"Data de Nascimento: {cliente['data_nascimento']}")
        print(f"Endereço: {cliente['endereco']}")
        print(f"Número de Contas: {len(cliente['contas'])}")
        print("-" * 50)
    print("==================================================")

# Nova Função: Listar Contas Correntes
def listar_contas_correntes(contas, clientes):
    """
    Lista todas as contas correntes cadastradas.
    Argumentos: contas (lista de dicionários), clientes (lista para buscar nomes).
    Retorna: None (apenas exibe).
    """
    if not contas:
        print("Nenhuma conta corrente cadastrada.")
        return

    print("\n================ LISTA DE CONTAS CORRENTES ================")
    for conta in contas:
        cliente = next((c for c in clientes if c['cpf'] == conta['cliente_cpf']), None)
        nome_cliente = cliente['nome'] if cliente else "Desconhecido"
        print(f"Agência: {conta['agencia']}")
        print(f"Número da Conta: {conta['numero']}")
        print(f"Titular: {nome_cliente}")
        print(f"Saldo: R$ {conta['saldo']:.2f}")
        print("-" * 50)
    print("==========================================================")

# Função Principal
def main():
    menu = """
    [d] Depositar
    [s] Sacar
    [e] Extrato
    [u] Criar Usuário
    [c] Criar Conta Corrente
    [lc] Listar Clientes
    [lcc] Listar Contas Correntes
    [q] Sair

    => """

    clientes = []
    contas = []
    LIMITE = 500
    LIMITE_SAQUES = 3
    LIMITE_OPERACOES = 20

    while True:
        opcao = input(menu).lower()

        if opcao == "d":
            if not contas:
                print("Nenhuma conta cadastrada! Crie uma conta primeiro.")
                continue
            conta = contas[0]
            valor = float(input("Informe o valor do depósito: "))
            conta['saldo'], conta['extrato'] = depositar(conta['saldo'], valor, conta['extrato'])

        elif opcao == "s":
            if not contas:
                print("Nenhuma conta cadastrada! Crie uma conta primeiro.")
                continue
            conta = contas[0]
            valor = float(input("Informe o valor do saque: "))
            conta['saldo'], conta['extrato'], conta['numero_saques'], conta['ultimo_saque_data'] = sacar(
                saldo=conta['saldo'],
                valor=valor,
                extrato=conta['extrato'],
                limite=LIMITE,
                numero_saques=conta['numero_saques'],
                limite_saques=LIMITE_SAQUES,
                ultimo_saque_data=conta['ultimo_saque_data']
            )

        elif opcao == "e":
            if not contas:
                print("Nenhuma conta cadastrada! Crie uma conta primeiro.")
                continue
            conta = contas[0]
            exibir_extrato(conta['saldo'], extrato=conta['extrato'])

        elif opcao == "u":
            criar_usuario(clientes)

        elif opcao == "c":
            criar_conta_corrente(clientes, contas)

        elif opcao == "lc":
            listar_clientes(clientes)

        elif opcao == "lcc":
            listar_contas_correntes(contas, clientes)

        elif opcao == "q":
            print("Saindo do sistema. Obrigado por usar nossos serviços!")
            break

        else:
            print("Operação inválida! Por favor, selecione uma opção válida.")

if __name__ == "__main__":
    main()