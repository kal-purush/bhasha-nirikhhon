from random import randint
from aEstrela import astar
from genetico import genetico
from state import State
from todosParaTodos import todosParaTodos

class SalvadorPlan:

    ACTIONS = ["N", "S", "L", "O", "NE", "NO", "SE", "SO"]

    MOVIMENTS = {
        "N" : (-1, 0),
        "S" : (1, 0),
        "L" : (0, 1),
        "O" : (0, -1),
        "NE" : (-1, 1),
        "NO" : (-1, -1),
        "SE" : (1, 1),
        "SO" : (1, -1)
    }

    ANTI_ACTIONS = {
        "N" : "S",
        "S" : "N",
        "L" : "O",
        "O" : "L", 
        "NE" : "SO",
        "NO" : "SE",
        "SE" : "NO",
        "SO" : "NE"
    }



    def __init__(self, maxRows, maxColumns, goal, initialState, maze, vitimas, name = "none", mesh = "square"):
        """
        Define as variaveis necessárias para a utilização do explorer plan por um unico agente.
        """
        self.walls = []
        self.maxRows = maxRows
        self.maxColumns = maxColumns
        self.initialState = initialState
        self.currentState = initialState
        self.maze = maze
        self.vitimas = vitimas
        self.goalPos = goal
        self.path = []
        self.tempo = 0
        self.actions = []

    def setWalls(self, walls):
        # row = 0
        # col = 0
        # for i in walls:
        #     col = 0
        #     for j in i:
        #         if j == 1:
        #             self.walls.append((row, col))
        #         col += 1
        #     row += 1
        pass
       
        
    def updateCurrentState(self, state):
         self.currentState = state

    def isPossibleToMove(self, toState):
        """Verifica se eh possivel ir da posicao atual para o estado (lin, col) considerando 
        a posicao das paredes do labirinto e movimentos na diagonal
        @param toState: instancia da classe State - um par (lin, col) - que aqui indica a posicao futura 
        @return: True quando é possivel ir do estado atual para o estado futuro """

        return True
        ## vai para fora do labirinto
        if (toState.col < 0 or toState.row < 0):
            return False

        if (toState.col >= self.maxColumns or toState.row >= self.maxRows):
            return False
        
        if len(self.walls) == 0:
            return True
        
        ## vai para cima de uma parede
        if (toState.row, toState.col) in self.walls:
            return False

        # vai na diagonal? Caso sim, nao pode ter paredes acima & dir. ou acima & esq. ou abaixo & dir. ou abaixo & esq.
        delta_row = toState.row - self.currentState.row
        delta_col = toState.col - self.currentState.col

        ## o movimento eh na diagonal
        if (delta_row !=0 and delta_col != 0):
            if (self.currentState.row + delta_row, self.currentState.col) in self.walls and (self.currentState.row, self.currentState.col + delta_col) in self.walls:
                return False
        
        return True

    def getNextPosition(self):
         """ Sorteia uma direcao e calcula a posicao futura do agente 
         @return: tupla contendo a acao (direcao) e o estado futuro resultante da movimentacao """
         if len(self.actions) > 0:
            movDirection = self.actions.pop(0)
         else: 
            return
         state = State(self.currentState.row + self.MOVIMENTS[movDirection][0], self.currentState.col + self.MOVIMENTS[movDirection][1])
         return movDirection, state

    def chooseAction(self):
        """ Escolhe o proximo movimento de forma aleatoria. 
        Eh a acao que vai ser executada pelo agente. 
        @return: tupla contendo a acao (direcao) e uma instância da classe State que representa a posição esperada após a execução
        """

        ## Tenta encontrar um movimento possivel dentro do tabuleiro 
        result = self.getNextPosition()
        if result != None:
            while not self.isPossibleToMove(result[1]):
                result = self.getNextPosition()

        return result

    def getCaminhos(self, base):
        return todosParaTodos(self.maze, self.vitimas, tuple(base))

    def selectVitimas(self, base, tempoMax):
        caminhos = self.getCaminhos(base)
        self.path = genetico(caminhos, self.vitimas, tempoMax)
        self.posToActions()

    def posToActions(self):
        self.actions = []
        for i in range(len(self.path)-1):
            action = (self.path[i+1][0] - self.path[i][0], self.path[i+1][1] - self.path[i][1])
            for chave, valor in self.MOVIMENTS.items():
                if action == valor:
                    self.actions.append(chave)

    def run_valid_action(self, previousAction):
        pass
        
    def run_invalid_action(self):
        pass

    def do(self):
        """
        Método utilizado para o polimorfismo dos planos

        Retorna o movimento e o estado do plano (False = nao concluido, True = Concluido)
        """
        
        nextMove = self.move()
        return (nextMove[1], self.goalPos == State(nextMove[0][0], nextMove[0][1]))   
    
     


        
       
        
        