from constanst import *
from random import randint
from moves import Move
# import sys

#sys.setrecursionlimit(50000)


class Labyrinth():
    """
    Autor: Kevin Cardona
    Fecha: febrero 17 2018
    Clase Laberinto
    """
    matriz = []

    def run(self):
        """
        Autor: Kevin Cardona
        Fecha: febrero 17 2018
        método encargado de poner en marcha el laberinto
        """
        self.create()

        print("***************** INICIO DEL JUEGO *******************************")
        self.draw()
        print("******************************************************************")

        try:
            self.play()
        except RecursionError:
            print("You loser")

    def move(self):
        """
        Autor: Kevin Cardona
        Fecha: febrero 17 2018
        método encargado de jugar
        """
        move = Move()
        position_x, position_y, find = move.read_matriz(self.matriz, position_mouse_x=self.position_mouse_x, position_mouse_y=self.position_mouse_y)

        return position_x, position_y, find

    def play(self):
        """

        :return:
        """
        position_x, position_y, find = self.move()

        if find:
            print("YOU WIN")
        else:
            self.matriz[position_x][position_y] = 'R'
            self.position_mouse_x = position_x
            self.position_mouse_y = position_y
            self.draw()
            self.play()

    def create(self):
        """
        Autor: Kevin Cardona
        Fecha: febrero 17 2018
        método encargado de poner en marcha el laberinto
        """

        # Se crea la matriz
        self.matriz = [' '] * ROWS_NUMBER

        for i in range(ROWS_NUMBER):
            self.matriz[i] = [' '] * COLUMNS_NUMBER

        # Cantidad de bloques en la matriz
        size_matriz = ROWS_NUMBER * COLUMNS_NUMBER
        blocks_amount = round(size_matriz / 5)

        for block in range(0, blocks_amount):
            ramdom_x, ramdom_y = self.position_random(ROWS_NUMBER, COLUMNS_NUMBER)

            if not self.matriz[ramdom_x][ramdom_y].strip(" "):
                self.matriz[ramdom_x][ramdom_y] = "X"

        # crear queso
        self.position_cheese_x, self.position_cheese_y = self.create_cheese()

        # crear raton
        self.position_mouse_x, self.position_mouse_y = self.create_mouse()

    def create_cheese(self):
        """
        Autor: Kevin Cardona
        Fecha: febrero 17 2018
        método encargado de crear el queso en la matriz
        """
        # ubicacion del queso
        ramdom_x, ramdom_y = self.position_random(ROWS_NUMBER, COLUMNS_NUMBER)
        if not self.matriz[ramdom_x][ramdom_y].strip(" "):
            self.matriz[ramdom_x][ramdom_y] = "Q"
        else:
            self.create_cheese()

        return ramdom_x, ramdom_y

    def create_mouse(self):
        """
        Autor: Kevin Cardona
        Fecha: febrero 17 2018
        método encargado de crear el raton en la matriz
        """
        # ubicacion del queso
        ramdom_x, ramdom_y = self.position_random(ROWS_NUMBER, COLUMNS_NUMBER)
        if not self.matriz[ramdom_x][ramdom_y].strip(" "):
            self.matriz[ramdom_x][ramdom_y] = "R"
        else:
            self.create_mouse()

        return ramdom_x, ramdom_y

    def draw(self):
        """
        Autor: Kevin Cardona
        Fecha: febrero 17 2018
        método encargado de dibujar el laberinto
        """
        for row in self.matriz:
            print(row, '\n')
        print("--------------------------------------------------------------------")

    def position_random(self, max_number_x, max_number_y):
        """
        Autor: Kevin Cardona
        Fecha: febrero 17 2018
        función auxiliar para obtener el ramdon de una posición en la matriz
        :param max_number_x: máximo numero para x
        :param max_number_y: máximo numero para y
        :return: random_x, random_y
        """
        random_x = randint(1, max_number_x) - 1
        random_y = randint(1, max_number_y) - 1

        return random_x, random_y

laberyrint = Labyrinth()
laberyrint.run()