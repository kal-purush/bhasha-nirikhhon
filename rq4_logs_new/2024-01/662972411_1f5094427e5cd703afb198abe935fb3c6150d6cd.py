from shapely import unary_union, Polygon

from Algorithms.BranchesAndBounds.Branch import Branch
from Algorithms.BranchesAndBounds.ParamsClasses.FlexibleBnBParams import FlexibleBnBParams


class TripleBnBParams(FlexibleBnBParams):
    def __init__(self,
                 P: Polygon,
                 init_circles: int,
                 SELF_INTER_W: float = 0,
                 OUTSIDE_W: float = 0.05,
                 CIRCLE_COUNT_W: float = 0.005,
                 COVERAGE_W: float = 1.5,
                 ANGLE_RESOLUTION: int = 6,
                 MOVE_MULTIPLIER: float = 1.5,
                 MOVE_SCHEDULE=(lambda x: x),
                 LITTLE_UNIQUE_RATE: float = 0.05):
        super().__init__(
            P=P,
            init_circles=init_circles,
            SELF_INTER_W=SELF_INTER_W,
            OUTSIDE_W=OUTSIDE_W,
            CIRCLE_COUNT_W=CIRCLE_COUNT_W,
            COVERAGE_W=COVERAGE_W,
            ANGLE_RESOLUTION=ANGLE_RESOLUTION,
            MOVE_MULTIPLIER=MOVE_MULTIPLIER,
            MOVE_SCHEDULE=MOVE_SCHEDULE)
        self.LITTLE_UNIQUE_RATE = LITTLE_UNIQUE_RATE

    def find_bad_circles(self, branch: Branch) -> list[int]:
        """
        Находит индексы 'плохих' кругов для данной ветви.

        :param branch: Ветвь.
        :return: Массив индексов.
        """

        # Плохие круги:
        #   1) Имеющий наименьшее пересечение с многоугольником.
        #   ##########################2) Имеющий наибольшее пересечение с другими кругами при небольшой доле (<const 0.3) уникальной площади.
        #   2) Имеющий малую долю (< const) уникальной площади.

        min_poly_inter = float('inf')  # Минимальное пересечение с многоугольником.
        min_poly_inter_ind = -1  # Соответствующий круг.

        little_unique = float('inf')  # Минимальная доля уникальной площади (из тех, что < const).
        little_unique_ind = -1  # Соответствующий круг.

        for i in range(len(branch.circles)):
            circle = branch.circles[i]  # Текущий кандидат на плохой круг.

            if branch.fixed[i]:
                continue
            cur_inter = branch.polygon.intersection(circle.polygon).area
            if cur_inter < min_poly_inter:
                min_poly_inter = cur_inter
                min_poly_inter_ind = i

            # Круги, которые недалеко от i-го.
            near = [c.polygon  # TODO: создать сетку, чтобы не делать такой сложный цикл
                    for c in branch.circles
                    if c != circle and
                    c.center.distance(circle.center) < 2 * circle.radius]

            # Это уникальная площадь внутри многоугольника, которую добавляет этот круг.
            unique_area = circle.polygon.difference(unary_union(near)).intersection(branch.polygon).area
            unique_area /= circle.polygon.area  # Доля уникальной площади, которую привносит круг.



            if unique_area < self.LITTLE_UNIQUE_RATE and unique_area < little_unique:
                little_unique = unique_area
                little_unique_ind = i




        ans = []
        if min_poly_inter_ind != -1:
            ans.append(min_poly_inter_ind)
        if little_unique_ind != -1:
            ans.append(little_unique_ind)
        return ans