import random


class A_Espia():
    #Estados:
        #0 | libre         | ' '
        #1 | obstáculo     |  + or '|' or '='
        #2 | ya explorado  |
        #3 | guardia       |  G
        #4 | tesoro        |  $
        #5 | cárcel        |  C

    # Almacena los estados en las 4 direcciones (arriba, derecha, abajo, izquierda)
    environment = [0, 0, 0, 0]
    # Almacena la posición del agente y,x
    position = [0, 1]
    # Donde se el agente construye su mapa
    agentMap = []

    def __init__(self, originalMap):
        self.originalMap = tuple(originalMap)
        self.workingMap = originalMap
        print("El agente está vivoooo")

        for i in range(15):
            self.randomDirection()

    def testEnv(self):
        print(' ', self.environment[0], ' ')
        print(self.environment[3], self.workingMap[self.position[0]][self.position[1]], self.environment[1])
        print(' ', self.environment[2], ' ')

    def randNum(self):
        num = random.randint(0, 3)
        return num

    def checkEnvironment(self):
        self.environment[0] = self.workingMap[self.position[0] - 1][self.position[1]]
        self.environment[1] = self.workingMap[self.position[0]][self.position[1] + 1]
        self.environment[2] = self.workingMap[self.position[0] + 1][self.position[1]]
        self.environment[3] = self.workingMap[self.position[0]][self.position[1] - 1]

        for index in range(4):
            element = self.environment[index]
            if element == ' ':
                self.environment[index] = 0
            elif element == '+' or element == '|' or element == '=':
                self.environment[index] = 1
            elif element == '':   #Falta analizar
                self.environment[index] = 2
            elif element == 'G':
                self.environment[index] = 3
            elif element == '$':
                self.environment[index] = 4
            elif element == 'C':
                self.environment[index] = 5
            else:
                print('Revisa tus caracteres')

        self.testEnv()

    def clearPosition(self):
        self.workingMap[self.position[0]][self.position[1]] = ' '
        pass

    def updatePosition(self):
        self.workingMap[self.position[0]][self.position[1]] = 'E'

    def moveUp(self):
        self.clearPosition()
        self.position[0] -= 1
        self.updatePosition()

    def moveRight(self):
        self.clearPosition()
        self.position[1] += 1
        self.updatePosition()

    def moveDown(self):
        self.clearPosition()
        self.position[0] += 1
        self.updatePosition()

    def moveLeft(self):
        self.clearPosition()
        self.position[1] -= 1
        self.updatePosition()

    def randomDirection(self):
        self.checkEnvironment()

        impossibleMove = True
        while impossibleMove:
            nextMove = self.randNum()
            if self.environment[nextMove] == 0:
                impossibleMove = False

        if nextMove == 0:
            self.moveUp()
        elif nextMove == 1:
            self.moveRight()
        elif nextMove == 2:
            self.moveDown()
        elif nextMove == 3:
            self.moveLeft()