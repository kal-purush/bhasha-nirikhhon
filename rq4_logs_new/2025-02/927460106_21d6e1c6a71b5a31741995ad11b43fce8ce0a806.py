from window import Window
from cell import Cell
import time

class Maze:
    def __init__(
            self,
            x1,
            y1,
            num_rows,
            num_cols,
            cell_size_x,
            cell_size_y,
            win: Window,
        ):
        self.x1 = x1
        self.y1 = y1
        self.num_rows = num_rows
        self.num_cols = num_cols
        self.cell_size_x = cell_size_x
        self.cell_size_y = cell_size_y
        self._cells = []
        self.win = win
        self._create_cells()

    def _create_cells(self): 
        for i in range(self.num_cols):
            col = []
            for j in range(self.num_rows):
                cell = Cell(
                        self.x1 + (i * self.cell_size_x),
                        self.x1 + (i * self.cell_size_x) + self.cell_size_x,
                        self.y1 + (j * self.cell_size_y),
                        self.y1 + (j * self.cell_size_y) + self.cell_size_y,
                        self.win
                    )
                col.append(cell)
            self._cells.append(col)
        for i in range(self.num_cols):
            for j in range(self.num_rows):
                self._draw_cell(i, j)


    def _draw_cell(self, i, j):
        cell = self._cells[i][j]
        cell.draw("white")
        self._animate()

    def _animate(self):
        self.win.redraw()
        time.sleep(.05)