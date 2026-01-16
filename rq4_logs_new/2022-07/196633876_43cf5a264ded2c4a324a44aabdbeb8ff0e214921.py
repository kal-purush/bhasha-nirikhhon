from domino.container import BaseContainer


class Board(BaseContainer):
    """
    Esta clase representa un panel en donde se pueden ubicar fichas

    Parametros
    ----------
    window(Window): tablero en donde se dibujaran las fichas
    vertical_scrollable(bool): Booleano que indica si las fichas se pueden mover
        en direccion vertical
    horizontal_scrollable(bool): Booleano que indica si las fichas se pueden
        mover en direccion horizontal
    """

    def __init__(self, window, vertical_scrollable=True, horizontal_scrollable=True):

        super().__init__(window)

        self.vertical_scrollable = vertical_scrollable
        self.horizontal_scrollable = horizontal_scrollable

    def input_handler(self, char):
        # el scrolling de las fichas se maneja con la variable zero_position que
        # es el corrimiento que se debe agregar a su posicion en el momento de
        # dibujarlas

        # manejando scrolling de las fichas
        if self.vertical_scrollable:
            if char == 259:  # UP
                self.zero_position[0] -= 1
            if char == 258:  # DOWN
                self.zero_position[0] += 1
        if self.horizontal_scrollable:
            if char == 260:  # RIGHT
                self.zero_position[1] -= 1
            if char == 261:  # LEFT
                self.zero_position[1] += 1
        if char == 114:  # r
            self.elements[self.linter][0].reflect()