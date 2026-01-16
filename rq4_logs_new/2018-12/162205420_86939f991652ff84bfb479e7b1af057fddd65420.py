################################################################################
#
# Este é o exercício da 1a avaliação da disciplina de IA.
#
# O código traz um esqueleto para a implementação da busca A* para
# resolver o problema do quebra-cabeça de 8 números.
#
# O Objetivo do exercício é implementar o que falta da busca no código abaixo.
# Os métodos que precisam ser implementados estão com a marcação "TODO" seguida
# de uma descrição do que precisa ser feito.
#
# Leia atentamente todo os comentários no código.
#
# Se o programa estiver executando corretamente, ele deve exibir todas as
# configurações do tabuleiro do quebra cabeça dos 8 números para sair do estado
# final até chegar ao objetivo.
#
################################################################################

import random
import copy

class Board(object):

    """
    Esta classe representa uma configuração do tabuleiro do
    quebra-cabeça. O tabuleiro é um estado no problema de busca.
    
    O tabuleiro tem 9 posições (em inglês tiles), sendo 8 posições dedicadas
    aos números de 1 até 8 e uma posição especial "x" que representa a posição
    vazia.
 
    O tabuleiro é representado de forma linear, por exemplo,
    [1, 2, 3, 4, 5, 6, 7, 8, "x"], que visualmente representa o tabuleiro:

                                   1  2  3
                                   4  5  6
                                   7  8  x
    """
    
    def __init__(self, tiles):
        """
        Construtor.

        tiles é uma lista com as posições do tabuleiro, por exemplo,
        [1, 2, 3, 4, 5, 6, 7, 8, "x"].
        """
        self.tiles = tiles

    def is_goal(self):
        # TODO: este método verifica se o tabuleiro atual é uma solução
        # do problema. O metódo deve retornar True se o tabuleiro for uma
        # solução e False se não for.
        goal = Board([1, 2, 3, 4, 5, 6, 7, 8, "x"])
        return self.__eq__(goal) 

    def heuristic(self):
        # TODO: este método calcula a função heurística para o estado
        # representado pelo tabuleiro. A heurística usada deve retornar o
        # valor da contagem de quantos números estão na posição errada em
        # relação ao objetivo. Por exemplo, o estado
        # [2, 1, 3, 4, 5, 6, 7, 8, "x"] tem valor 2 para heurística, pois
        # o número "1" está na posição errada e o número "2" também.
        # Este método deve retornar o valor da função heurística.
        goal = [1, 2, 3, 4, 5, 6, 7, 8, "x"]
        cont = 0
        for a in range (len(self.tiles)):
            if(goal[a] != self.tiles[a]):
                cont+=1
        return cont

    def get_neighbors(self):
        # TODO: Este método deve retornar uma lista com os vizinhos do estado
        # representado pelo tabuleiro. Os vizinhos são os possíveis novos
        # tabuleiros resultantes da movimentação das peças do tabuleiro atual.
        # A lista retornada é uma lista de objetos da classe Board.
        moves = []
        x_index = self.tiles.index("x")
        move_to = {
            "line_above": x_index - 3, 
            "line_below": x_index + 3, 
            "left_column": x_index - 1,
            "right_column": x_index + 1
        }
        column = {
            "left":move_to["left_column"] % 3,
            "right":move_to["right_column"] % 3
        }
        limit ={
            "up": 0,
            "down": 8,
            "sides": x_index % 3
        }

        if (move_to["line_above"] >= limit["up"]):
            moves.append(move_to["line_above"])

        if (move_to["line_below"] <= limit["down"]):
            moves.append(move_to["line_below"])

        if column["left"] < limit["sides"]:
            moves.append(move_to["left_column"])

        if column["right"] > limit["sides"]:
            moves.append(move_to["right_column"])
        
        return self.create_neighbors(moves)


    def create_neighbors(self,moves):
        #Novo: Este método recebe como parâmetro os possíveis movimentos de x
        # o retorno desse método deve ser uma lista com os vizinhos de Board    
        neighbors = []
        x_index = self.tiles.index("x")
        for value in moves:
            temp = copy.copy(self.tiles)
            temp[x_index] = temp[value]
            temp[value] = "x"
            neighbors.append(Board(temp))
        return neighbors

    # Os métodos a seguir dessa classe não devem ser modificados
    def __eq__(self, other):
        return self.tiles == other.tiles

    def __hash__(self):
        return hash(tuple(self.tiles))

    def __str__(self):
        return str(self.tiles)

    def __repr__(self):
        return str(self.tiles)

    def print_board(self):
        print(self.tiles[:3])
        print(self.tiles[3:6])
        print(self.tiles[6:])

    


class Node(object):
    """
    Esta classe representa um nó na busca. Cada nó contém um estado (tabuleiro),
    um custo e o pai do nó, este último é a referência para um outro nó.

    Esta classe não deve ser modificada.
    """
    def __init__(self, state, cost):
        """
        Construtor.

        state é um objeto da classe Board.
        cost é um número que representa o custo do nó.
        """
        self.state = state
        self.cost = cost
        self.parent = None

    def __str__(self):
        return str(self.state.tiles) + " - " + str(self.cost)

    def __repr__(self):
        return str(self.state.tiles) + " - " + str(self.cost)


class AStar(object):
    """
    Esta classe é responsável por fazer a busca A*. Ela recebe um estado
    inicial no construtor indicando a configuração inicial do tabuleiro do
    quebra cabeça.
    """
    def __init__(self, initial_state):
        """
        Construtor.

        initial_state é o estado inicial, ou seja, a configuração inicial do
        tabuleiro.

        No construtor também é iniciada a fronteira e o conjunto dos
        explorados. Note que a fronteira é uma lista de objetos da classe Node.
        """
        self.initial_state = initial_state
        self.frontier = [Node(self.initial_state, 0 + self.initial_state.heuristic())]
        self.explored = set()
        self.current_node = None

    def choose_from_frontier(self):
        # TODO: Este método remove e retorna o nó com menor custo da
        # fronteira.
        current_cost = self.frontier[0]
        cont = 0
        for a in range(len(self.frontier)):
            if self.frontier[a].cost < current_cost.cost:
                current_cost = self.frontier[a]
                cont = a
        return self.frontier.pop(cont)

    def update_frontier(self):
        # TODO: Este método é executado após ser escolhido um estado da
        # fronteira. No método search (encontrado mais abaixo), o estado
        # selecionado é atribuido a variável self.current_node. Assim,
        # esse método deve atualizar a fronteira com os vizinhos do estado
        # contido em self.current_node.
        #
        # Ao adicionar um nó na fronteira, lembre-se do cálculo do seu custo,
        # de gravar a referência para o nó pai e das regras existentes para
        # adicionar ou não um novo nó.
        #
        # Este método não precisa retornar a fronteira, já que ela pode ser
        # acessada em toda classe através da variável self.frontier.
        neighbors = self.current_node.state.get_neighbors()
        path_cost = self.current_node.cost - self.current_node.state.heuristic() + 1
        new_node_cost = path_cost
        
        for neighbor in neighbors:
            new_node_cost += neighbor.heuristic()
            
            if self.is_neighbor_in_frontier(neighbor):
                node = self.frontier[self.get_frontier_index(neighbor)]
                
                if new_node_cost < node.cost:
                    node.parent = self.current_node
                    node.cost = new_node_cost
            
            elif not (neighbor in self.explored):
                new_node = Node(neighbor, new_node_cost)
                new_node.parent = self.current_node
                self.frontier.append(new_node)       
            
            new_node_cost -= neighbor.heuristic()

    def get_frontier_index(self, neighbor):
        # Novo: esse metodo recebe como parâmetro um vizinho e encontra o nó da fronteira
        # com o estado igual ao vizinho.
        # O retorno desse metodo deve ser o índice do nó cujo estado é igual ao vizinho
        # recebido como parâmetro
        for index in range(len(self.frontier)):
            if self.frontier[index].state == neighbor:
                return index


    def is_neighbor_in_frontier(self, neighbor):
        """
        Este método avalia se algum nó da fronteira contém o estado (neighbor).
        Você pode utilizar este método para implementar o update_frontier.
        """
        for node in self.frontier:
            if node.state == neighbor:
                return True
        
        return False

    def get_path(self, node):
        # TODO: Este método retorna o caminho feito do estado inicial até o
        # nó passado como parâmetro (node).
        #
        # O retorno deve ser uma lista, que começa com o estado inicial e
        # termina com o estado do nó passado como parâmetro (node).
        #
        # Por exemplo, se a partir de um nó A, para chegar a um nó D, o
        # o algoritmo passou pelos nós B e C, então o retorno deve ser a lista
        # [A, B, C, D].
        #
        # Passando o nó objetivo encontrado pela busca como parâmetro dessa
        # função, tem-se como resultado o caminho completo para sair do estado
        # inicial e chegar no objetivo.
        if (node.parent.state == self.initial_state): 
            node_list = []
            node_list.append(self.initial_state)
            node_list.append(node.state)
            return node_list
            
        node_list = self.get_path(node.parent)
        node_list.append(node.state)
        return node_list

    def search(self):
        """
        Este método executa a busca A* para resolver o problema do
        quebra-cabeça de 8 números.

        Atenção: Para algumas configurações de tabuleiro, a solução pode ser
        impossível, causando um loop infinito.
        """
        while True:
            if len(self.frontier) == 0:
                return False

            self.current_node = self.choose_from_frontier()
            
            self.explored.add(self.current_node.state)

            if self.current_node.state.is_goal():
                return self.current_node

            self.update_frontier()
            
        
if __name__ == "__main__":
    # Para testar o algoritmo, o quebra-cabeça pode ser iniciado
    # de forma aleatória ou com um tabuleiro fixo.
    #
    # Abaixo temos as duas opções. Comente ou descomente as linhas
    # correspondentes para usar a opção desejada.
    #
    # Para testes iniciais, aconselho usar o tabuleiro fixo, pois
    # essa é uma configuração que temos certeza que tem solução.


    # Iniciando o quebra-cabeça de forma aleatória
    #tiles = [1, 2, 3, 4, 5, 6, 7, 8, "x"]
    
    #random.shuffle(tiles)
    #print(tiles)
    # Iniciando o quebra-cabeça com um tabuleiro fixo
    tiles = [3, 2, 8, 1, 5, 4, 7, 6, "x"]
    #tiles = [1, 2, 3, 4, 5, 6, 7, 8, "x"]
    #tiles = [1,2,3,4,"x",6,7,5,8]
    #tiles = [1,"x",3,4,2,6,7,5,8]
    #tiles = [1,3,6,4,"x",2,7,5,8]
    #tiles = ["x",1,6,4,3,2,7,5,8]
    #tiles = [2,7,8,6,"x",4,5,3,1]
    #tiles = [3, 1, 4, 2, 'x', 7, 5, 8, 6]
    #tiles = [3, 2, 4, 'x', 7, 6, 5, 1, 8]
    initial_state = Board(tiles)

    astar = AStar(initial_state)
    final_node = astar.search()
    path = astar.get_path(final_node)
    print("caminho")
    
    for state in path:
        state.print_board()
        print("---")
    print(len(path))