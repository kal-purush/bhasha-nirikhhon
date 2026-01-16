'''
Regras:
1) Transferir bottle1 -> bottle2
2) Transferir bottle1 -> bottle3
3) Transferir bottle2 -> bottle1
4) Transferir bottle2 -> bottle3
5) Transferir bottle3 -> bottle1
6) Transferir bottle3 -> bottle2

Ordem de aplicação de regras: crescente
'''



from src.Node import Node


class BFS:
    def __init__(self, bottle1, bottle2, bottle3):
        self.bottles = [bottle1, bottle2, bottle3]
        self.id = 1
        self.root_node = Node(0, None, bottle1, bottle2, bottle3)
        self.caminho = []
        self.openedList = []
        self.closedList = []
        self.openedList.append(self.root_node)
        self.rules = [self.rule_1, self.rule_2, self.rule_3, self.rule_4, self.rule_5, self.rule_6]

    def start(self):
        while len(self.openedList) != 0:
            node = self.openedList.pop(0)
            self.retornar_quantidade(node.get_bottles_quantity())
            node.show_information()
            if not self.is_success(node):
                current_rule = node.get_rule()
                while current_rule < 6:
                    current_bottle_quantity = node.get_bottles_quantity()
                    if self.aplicar_regra(current_rule, node):
                        print('aplicou regra {}'.format(current_rule))
                        new_node = Node(self.id, node, self.bottles[0], self.bottles[1], self.bottles[2])
                        node.add_child(new_node)
                        self.openedList.append(new_node)
                        print('adicionou o nó {}'.format(new_node.get_bottles_quantity()))
                    self.retornar_quantidade(current_bottle_quantity)
                    current_rule += 1
                self.closedList.append(node.get_bottles_quantity())

            else:  #Construir caminho inverso
                print('sucesso')
                break
            print('volta pro while')


    def retornar_quantidade(self, current_quantity):
        for i in range(3):
            self.bottles[i].set_quantity(current_quantity[i])


    def aplicar_regra(self, regra, node):
        novo_balde = self.bottles.copy()
        #sucesso = novo_balde[0].transfer(novo_balde[1])
        sucesso = self.rules[regra](novo_balde)
        nova_quantidade = [novo_balde[0].getCurrentQuantity(),
                           novo_balde[1].getCurrentQuantity(),
                           novo_balde[2].getCurrentQuantity()]
        if sucesso and not self.verifica_caminho(node, nova_quantidade):
            self.bottles = novo_balde
            return True
        return False

    def verifica_caminho(self, current_node, new_node):
        aux_node = current_node
        while aux_node is not None:
            if aux_node.get_bottles_quantity() == new_node:
                return True
            aux_node = aux_node.get_father()
        return False

    def rule_1(self, novo_balde):
        # return self.bottles[0].transfer(self.bottles[1])
        return novo_balde[0].transfer(novo_balde[1])

    def rule_2(self, novo_balde):
        # return self.bottles[0].transfer(self.bottles[2])
        return novo_balde[0].transfer(novo_balde[2])

    def rule_3(self, novo_balde):
        # return self.bottles[1].transfer(self.bottles[0])
        return novo_balde[1].transfer(novo_balde[0])

    def rule_4(self, novo_balde):
        # return self.bottles[1].transfer(self.bottles[2])
        return novo_balde[1].transfer(novo_balde[2])

    def rule_5(self, novo_balde):
        # return self.bottles[2].transfer(self.bottles[0])
        return novo_balde[2].transfer(novo_balde[0])

    def rule_6(self, novo_balde):
        # return self.bottles[2].transfer(self.bottles[1])
        return novo_balde[2].transfer(novo_balde[1])

    @staticmethod
    def is_success(node):
        # node.show_information()
        node_quantity = node.get_bottles_quantity()
        if (node_quantity[0] == 4 and node_quantity[1] == 4 and node_quantity[2] == 0):
            return True
        return False

    def show_information(self):
        self.root_node.show_information()