import time
import random
from cell import Cell
class Maze:
    def __init__(self, x1, y1, num_rows, num_cols, cell_size_x, cell_size_y, win=None, seed=None):
        self.x1 = x1
        self.y1 = y1
        self.num_rows = num_rows
        self.num_cols = num_cols
        self.cell_size_x = cell_size_x
        self.cell_size_y = cell_size_y
        self._win = win
        self._create_cells()
        self._break_walls()

    def _create_cells(self):
        self._cells = []
        for i in range(0, self.num_cols):
            self._cells.append([])
        for node in self._cells:
            for i in range(0, self.num_rows):
                node.append(Cell(self._win))

        self._break_entrance_and_exit()
        for i in range(0, len(self._cells)):
            for j in range(0, len(self._cells[i])):
                self._draw_cell(i, j)

    def _draw_cell(self, i, j):
        if self._win is None:
            return
        cell = self._cells[i][j]
        if i == 0 and j == 0:
            cell.draw(self.x1, self.y1, (self.x1+self.cell_size_x), (self.y1+self.cell_size_y))
            self._animate()
        else:
            x = self.x1 + (i * self.cell_size_x)
            y = self.y1 + (j * self.cell_size_y)
            cell.draw(x, y, (x+self.cell_size_x), (y+self.cell_size_y))
            self._animate()

    def _animate(self):
        if self._win is None:
            return
        self._win.redraw()
        time.sleep(0.01)
    
    def _break_entrance_and_exit(self):
        start = self._cells[0][0]
        end = self._cells[-1][-1]
        start.has_left_wall = False
        end.has_right_wall = False
    
    def break_walls(self, i, j):