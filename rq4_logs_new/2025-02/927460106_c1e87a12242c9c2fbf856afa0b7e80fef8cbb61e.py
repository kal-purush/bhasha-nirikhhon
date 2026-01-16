from point import Point
from window import Window
from line import Line


class Cell:
    def __init__(
        self, 
        x1, 
        x2, 
        y1, 
        y2, 
        win: Window,
        has_left_wall=True,
        has_right_wall=True,
        has_top_wall=True,
        has_bottom_wall=True
    ):
        self.has_left_wall = has_left_wall
        self.has_right_wall = has_right_wall
        self.has_top_wall = has_top_wall
        self.has_bottom_wall = has_bottom_wall
        self._x1 = x1
        self._x2 = x2
        self._y1 = y1
        self._y2 = y2
        self._win = win

    def draw(self, fill_color):
        if self.has_left_wall:
            point_1 = Point(self._x1, self._y1)
            point_2 = Point(self._x1, self._y2)
            left_wall = Line(point_1, point_2)
            self._win.draw_line(left_wall, fill_color)
        
        if self.has_right_wall:
            point_3 = Point(self._x2, self._y1)
            point_4 = Point(self._x2, self._y2)
            right_wall = Line(point_3, point_4)
            self._win.draw_line(right_wall, fill_color)

        if self.has_top_wall:
            point_5 = Point(self._x1, self._y1)
            point_6 = Point(self._x2, self._y1)
            top_wall = Line(point_5, point_6)
            self._win.draw_line(top_wall, fill_color)

        if self.has_bottom_wall:
            point_7 = Point(self._x1, self._y2)
            point_8 = Point(self._x2, self._y2)
            bottom_wall = Line(point_7, point_8)
            self._win.draw_line(bottom_wall, fill_color)