#Autores: Kristian Antonio Frey, Edson Renan Baierle, Iury da Silva, João Vitor Winkelmann.
import pandas as pd

class No: 
  def __init__(self, dados): #No construtor
    self.dados = dados
    self.proximo = None

  def mostrar_no(self): #Mostrar o que tem na lista, conforme posição
    print(f"Produto(nome): {self.dados['nome']}, Estoque: {self.dados['estoque']}, Vendidos: {self.dados['vendidos']}")

class ListaEncadeada: 
  def __init__(self):
    self.primeiro = None

  def verifica_vazia(self): #Verificação se está vazia return true sem está e false se não
    if self.primeiro == None:
      return True
    else:
      return False

  def inserir_produto(self, estoque, vendidos, nome):  #Iserção dos produtos/lanches 
    dados = {} #objeto para guardar nome, estoque, vendidos, necessario para funcionar excel
    dados['nome'] = nome
    dados['estoque'] = estoque
    dados['vendidos'] = vendidos
    if self.verifica_vazia() == True:
      novo = No(dados)
      novo.proximo = self.primeiro
      self.primeiro = novo
    else:
      atual = self.primeiro
      novo = No(dados)
      while atual.proximo is not None:
           atual = atual.proximo
      atual.proximo = novo

  def buscar_produto(self, nome): #while para otimização do processo de busca de um produto
    atual = self.primeiro
    while atual is not None:
     if atual.dados['nome'] == nome:
       return atual
     atual = atual.proximo
    return None

  def venda(self, nome, quantidade): #responsavel pela diminuição na quantidade e reposisiona-lo na venda
    produto = self.buscar_produto(nome)
    if produto is not None:
      produto.dados['vendidos'] += quantidade
      produto.dados['estoque'] -= quantidade
    else:
      print(f"Produto {nome} não encontrado")

  def mostrar_lista(self): #mostrar todos os itens da lista/lanches
    if self.verifica_vazia() == True:
      print("Sem Produtos")
    else:
      atual = self.primeiro
      while atual is not None:
        atual.mostrar_no()
        atual = atual.proximo

  def mostrar_em_estoque(self):#Mostrar produtos com estoque maior que 0
    if self.verifica_estoque() == False:
      print("Sem Produtos em Estoque")
    else:
      atual = self.primeiro
      while atual is not None:
        if atual.dados['estoque'] > 0:
          atual.mostrar_no()
        atual = atual.proximo

  def set_estoque(self, nome, quantidade):
    # Atualiza a quantidade em estoque de um produto com o nome informado na lista encadeada para a quantidade informada
    produto = self.buscar_produto(nome)
    if produto is not None:
        produto.dados['estoque'] = quantidade
    else:
        print(f"Produto {nome} não encontrado")
      
  def add_estoque(self, nome, quantidade):
    # Aumenta a quantidade em estoque de um produto com o nome informado na lista encadeada pela quantidade informada
    produto = self.buscar_produto(nome)
    if produto is not None:
        produto.dados['estoque'] += quantidade
    else:
        print(f"Produto {nome} não encontrado")

  def remove_estoque(self, nome, quantidade):
    # Diminui a quantidade em estoque de um produto com o nome informado na lista encadeada pela quantidade informada
    produto = self.buscar_produto(nome)
    if produto is not None:
        produto.dados['estoque'] -= quantidade
    else:
        print(f"Produto {nome} não encontrado")
      
  def verifica_estoque(self): #verifica se a estoque.
    atual = self.primeiro
    while atual is not None:
      if atual.dados['estoque'] >= 0:
        return True
    return False

  def exportar_excel(self, nome_arquivo):
    # Exporta os dados do produto na lista encadeada para uma tabela do Excel com o nome do arquivo informado
    produtos = []
    atual = self.primeiro
    while atual is not None:
        produtos.append(atual.dados)
        atual = atual.proximo

    df = pd.DataFrame(produtos)
    df.to_excel(nome_arquivo, index=False)


lista = ListaEncadeada()
  
while True:
    print("Selecione uma opção:")
    print("1. Adicionar produto")
    print("2. Buscar produto")
    print("3. Vender produto")
    print("4. Mostrar todos os produtos")
    print("5. Mostrar produtos em estoque")
    print("6. Alterar estoque de produto")
    print("7. Adicionar estoque de produto")
    print("8. Remover estoque de produto")
    print("9. Exportar para Excel")
    print("10. Sair")

    opcao = input("Opção selecionada: ")
    
    if opcao == "1":
        nome = input("Nome do produto: ")
        estoque = int(input("Quantidade em estoque: "))
        vendidos = int(input("Quantidade vendida: "))
        lista.inserir_produto(estoque, vendidos, nome)
        print("Produto adicionado com sucesso.")
    elif opcao == "2":
        nome = input("Nome do produto: ")
        produto = lista.buscar_produto(nome)
        if produto is not None:
            print("Produto encontrado:")
            produto.mostrar_no()
        else:
            print("Produto não encontrado.")
    elif opcao == "3":
        nome = input("Nome do produto: ")
        quantidade = int(input("Quantidade vendida: "))
        lista.venda(nome, quantidade)
        print("Venda realizada com sucesso.")
    elif opcao == "4":
        lista.mostrar_lista()
    elif opcao == "5":
        lista.mostrar_em_estoque()
    elif opcao == "6":
        nome = input("Nome do produto: ")
        quantidade = int(input("Nova quantidade em estoque: "))
        lista.set_estoque(nome, quantidade)
        print("Estoque atualizado com sucesso.")
    elif opcao == "7":
        nome = input("Nome do produto: ")
        quantidade = int(input("Quantidade a ser adicionada ao estoque: "))
        lista.add_estoque(nome, quantidade)
        print("Estoque atualizado com sucesso.")
    elif opcao == "8":
        nome = input("Nome do produto: ")
        quantidade = int(input("Quantidade a ser removida do estoque: "))
        lista.remove_estoque(nome, quantidade)
        print("Estoque atualizado com sucesso.")
    elif opcao == "9":
        nome_arquivo = input("Digite o nome do arquivo de destino: ")
        nome_arquivo += '.xlsx'
        lista.exportar_excel(nome_arquivo)
    elif opcao == "10":
        print("Saindo do programa...")
        break
    else:
        print("Opção inválida. Tente novamente.")