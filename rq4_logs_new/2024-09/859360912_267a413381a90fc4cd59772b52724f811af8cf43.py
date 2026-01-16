import os

lista = []
i = 0

def inserir():      
    compra = input("Insira na lista: ")
    global i
    i += 1
    try: 
        lista.append(compra)
        os.system('cls' if os.name == 'nt' else 'clear')
        print("Item adicionado à sua lista!")
    except:
        print("Deu erro aqui dog")    

def apagar():
    indice = int(input("Informe o índice a ser apagado: "))

    try:
        del lista[indice]
        os.system('cls' if os.name == 'nt' else 'clear')
        print("Item deletado da lista!")
    except:
        print("O índice informado não possui nada!")

def listar():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("__Lista de Compras__")
    for item in lista:
        print(item)

while True:
    print("O que deseja fazer?")
    print("1 - Inserir")
    print("2 - Apagar")
    print("3 - Listar")
    print("0 - Sair")
    opcao = input()
    if opcao == '1':
        inserir()
    elif opcao == '2':
        apagar()
    elif opcao == '3':
        listar()
    elif opcao == '0':
        os.system('cls' if os.name == 'nt' else 'clear')
        print("Sistema finalizado!")
        break
    else:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("Informe uma opção válida\n")


