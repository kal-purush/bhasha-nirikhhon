import math
from typing import Tuple

class Circle:
    '''
    Representation of a circle on a coordinate plane

    Parameters
    ----------
    center : Tuple[float,float]
        x y coordinates of the center of the circle
    radius : float
        radius of the circle
    '''

    x : float
    y : float
    radius : float

    def __init__(self, center:Tuple[float,float], radius:float):
        self.x, self.y = center
        self.radius = radius

        self._min_x = self.x - self.radius
        self._max_x = self.x + self.radius
        self._min_y = self.y - self.radius
        self._max_y = self.y + self.radius

    def is_inside(self, point:Tuple[float,float]):
        '''Determine if point lies inside the circle.

        Parameters
        ----------
        point : Tuple[float,float]
            x y coordinates of point to check

        Returns
        -------
        bool
            True if point lies inside the circle
        '''
        p_x, p_y = point
        dist_from_center = math.sqrt( (p_x-self.x)**2 + (p_y-self.y)**2 )

        # TODO: add tolerance
        return dist_from_center <= self.radius


class Square:
    '''
    Representation of an axis-aligned square on a coordinate plane

    Parameters
    ----------
    center : Tuple[float,float]
        x y coordinates of the center of the square
    side_length : float
        length of one side of the square
    '''

    x : float
    y : float
    side_length : float

    def __init__(self, center:Tuple[float,float], side_length:float):
        self.x, self.y = center
        self.side_length = side_length

        self._half_length = self.side_length / 2
        self._min_x = self.x-self._half_length
        self._max_x = self.x+self._half_length
        self._min_y = self.y-self._half_length
        self._max_y = self.y+self._half_length

    def is_inside(self, point:Tuple[float,float]):
        '''Determine if point lies inside the square.

        Parameters
        ----------
        point : Tuple[float,float]
            x y coordinates of point to check

        Returns
        -------
        bool
            True if point lies inside the square
        '''
        p_x, p_y = point
        # TODO: add tolerance
        return p_x >= self._min_x and p_x <= self._max_x and \
                p_y >= self._min_y and p_y <= self._max_y