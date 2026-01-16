import math

# Lista em PILHA
class Stack:
    def __init__(self):
        self.items = []

    def is_empty(self):
        return len(self.items) == 0

    def push(self, item):
        self.items.append(item)

    def pop(self):
        if not self.is_empty():
            return self.items.pop()
        else:
            return None

    def peek(self):
        if not self.is_empty():
            return self.items[-1]
        else:
            return None

    def size(self):
        return len(self.items)


lista = Stack()
num = 140
while num > 0:
    resto = num % 2
    num = num // 2
    lista.push(resto)

# while not lista.is_empty():
#     print(lista.pop())



# LISTA EM FILA

class Fila():
    def __init__(self):
        self.data = []

    def inserir(self, x):
        self.data.append(x)

    def remover(self):
        if len(self.data) > 0:
            return self.data.pop(0)

    def top(self):
        if len(self.data) > 0:
            return self.data[0]

    def empty(self):
        return not len(self.data) > 0

fila = Fila()
fila.inserir(1)
fila.inserir(2)
fila.inserir(3)

fila.remover()
# print(fila.data)

# Exercício

prioritaria = Fila()
normal = Fila()

class Person:
    def __init__(self, age):
        self.age = age

persons = [Person(20), Person(30), Person(40), Person(50), Person(60), Person(70)]

def gerenciamentoFilas(listaPessoasNaFila, saiuDaFila):

    if saiuDaFila > len(listaPessoasNaFila):
        print("The number of people leaving is not right.")
        return

    for person in persons:
        if person.age < 60:
            normal.inserir(person)
        else:
            prioritaria.inserir(person)

    if saiuDaFila > 0:
        count = 0
        pessoasQueSairamDaFila = 0
        while count < saiuDaFila:
            count += 1
            pessoasQueSairamDaFila += 1
            if pessoasQueSairamDaFila == 3:
                normal.remover()
                pessoasQueSairamDaFila = 0
            elif prioritaria.empty():
                normal.remover()
            else:
                prioritaria.remover()


gerenciamentoFilas(persons, 0)
print([p.age for p in prioritaria.data])
print([p.age for p in normal.data])

