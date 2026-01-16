# sequencial = []
# sequencial.append(7) # add 7 ao final da lista / reproduzir na lista encadeada

from node import Node


class LinkedList:
    def __init__(self):
        # head identifica o primeiro elemento da lista
        self.head = None
        self._size = 0 # O tamanho da lista tbm pode ser obtido percorrendo a lista


    # função que permite add itens ao final da lista
    def append(self, elem): # Complexidade O(n) - pior caso
        if self.head:
            pointer = self.head
            while(pointer.next):
                pointer = pointer.next
            pointer.next = Node(elem)
        else:
            # primeira inserção
            self.head = Node(elem)
        self._size += 1

    def __len__(self): # Complexidade O(1)
        # Retorna o tamanho da lista
        return self._size # usa-se o inderline pra indicar que esta variável não deve ser acessada diretamente

    # Para acessar um elemento na lista
    def get(self, index):
        # a = lista.get(9)
        pass

    # Altera elemento
    def _set(self, index, elem):
        # lista.set(5, 9)
        pass

    def __getitem__(self, index): # Complexidade O(n)
        # a = lista(9)
        pointer = self.head
        for i in range(index):
            if pointer:
                pointer = pointer.next
            else:
                raise Indexerror("list index out of range")
        if pointer:
            return pointer.data
        else:
            raise Indexerror("list out of range")


    def __setitem__(self, index, elem): # Complexidade O(n)
        # lista(5) = 9
        # lista.set(5, 9) Sem sobrecarga de operador. Criando a função set acima
        pointer = self.head
        for i in range(index):
            if pointer:
                pointer = pointer.next
            else:
                raise Indexerror("list out of range")
        if pointer:
            pointer.data = elem
        else:
            raise Indexerror("list out of range")

    def index(self, elem): # Complexidade O(n)
        """Retorna o indice do elemento na lista"""
        pointer = self.head
        i = 0
        while(pointer):
            if pointer.data == elem:
                return i
            pointer = pointer.next
            i += 1
        raise ValueError(f"{elem} não está na lista")

lista = LinkedList()
lista.append(7)
lista.append(80)