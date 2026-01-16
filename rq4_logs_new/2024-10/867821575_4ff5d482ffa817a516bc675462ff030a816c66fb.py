from datetime import date
from abc import ABC, abstractmethod

# Classe base para itens da biblioteca
class ItemBiblioteca:
    def __init__(self, idItem):
        self.idItem = idItem

    def getIdItem(self):
        return self.idItem

# Subclasse de ItemBiblioteca para Livro
class Livro(ItemBiblioteca):
    def __init__(self, idItem, titulo, autor, ISBN, categoria, estado="disponível"):
        super().__init__(idItem)
        self.titulo = titulo
        self.autor = autor
        self.ISBN = ISBN
        self.categoria = categoria
        self.estado = estado

    def emprestar(self):
        if self.estado == "disponível":
            self.estado = "emprestado"
        else:
            print(f"Livro {self.titulo} não está disponível para empréstimo.")

    def devolver(self):
        if self.estado == "emprestado":
            self.estado = "disponível"
        else:
            print(f"Livro {self.titulo} não foi emprestado.")

    def reservar(self):
        if self.estado == "disponível":
            self.estado = "reservado"
        else:
            print(f"Livro {self.titulo} não pode ser reservado no momento.")

# Interface IUtilizador
class IUtilizador(ABC):
    @abstractmethod
    def cadastrar(self):
        pass

    @abstractmethod
    def consultarDisponibilidadeLivro(self, livro):
        pass

    @abstractmethod
    def gerarRelatorio(self):
        pass

# Classe Utilizador que implementa IUtilizador
class Utilizador(IUtilizador):
    def __init__(self, nome, endereco, telefone, idUtilizador):
        self.nome = nome
        self.endereco = endereco
        self.telefone = telefone
        self.idUtilizador = idUtilizador

    def cadastrar(self):
        print(f"Utilizador {self.nome} cadastrado com sucesso.")

    def consultarDisponibilidadeLivro(self, livro):
        return livro.estado

    def gerarRelatorio(self):
        pass  # Para ser implementado nas subclasses se necessário

# Subclasse Aluno de Utilizador
class Aluno(Utilizador):
    pass

# Subclasse Professor de Utilizador
class Professor(Utilizador):
    pass

# Classe Empréstimo
class Emprestimo:
    def __init__(self, dataEmprestimo, dataDevolucao, livro, Utilizador):
        self.dataEmprestimo = dataEmprestimo
        self.dataDevolucao = dataDevolucao
        self.livro = livro
        self.Utilizador = Utilizador

    def registrarEmprestimo(self):
        self.livro.emprestar()
        print(f"Empréstimo registrado: {self.livro.titulo} emprestado para {self.Utilizador.nome}.")

    def registrarDevolucao(self):
        self.livro.devolver()
        print(f"Devolução registrada: {self.livro.titulo} devolvido por {self.Utilizador.nome}.")

# Classe Relatório
class Relatorio:
    def __init__(self, dataGeracao, listaLivrosEmprestados=[], listaLivrosDevolvidos=[]):
        self.dataGeracao = dataGeracao
        self.listaLivrosEmprestados = listaLivrosEmprestados
        self.listaLivrosDevolvidos = listaLivrosDevolvidos

    def gerarRelatorioEmprestimos(self):
        print("Relatório de Empréstimos:")
        for livro in self.listaLivrosEmprestados:
            print(f"Livro: {livro.titulo}, Estado: {livro.estado}")

    def gerarRelatorioDevolucoes(self):
        print("Relatório de Devoluções:")
        for livro in self.listaLivrosDevolvidos:
            print(f"Livro: {livro.titulo}, Estado: {livro.estado}")