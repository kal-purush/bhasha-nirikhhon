tarefas = []

while True:
    opcao = input("Escolha uma opção: (1) Adicionar tarefa (2) Ver tarefas (3) Sair: ")
    
    if opcao == '1':
        tarefa = input("Digite a tarefa a ser adicionada: ")
        tarefas.append(tarefa)
    elif opcao == '2':
        print("Tarefas:")
        for idx, tarefa in enumerate(tarefas, start=1):
            print(f"{idx}. {tarefa}")
    elif opcao == '3':
        break
    else:
        print("Opção inválida. Tente novamente.")