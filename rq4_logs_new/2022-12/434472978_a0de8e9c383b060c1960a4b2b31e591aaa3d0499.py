from collections import deque

from daysolvers.base import SolverBase


class Day12Solver(SolverBase):
    day_number = 12
    iter = 0

    def part1(self):
        self.grid = [[*line] for line in self.get_daily_input(True, False)]
        start = self.__find_coords("S")
        end = self.__find_coords("E")
        shortest = self.__routes_to_end(start, end, [])
        return shortest

    def part2(self):
        self.grid = [[*line] for line in self.get_daily_input(True, False)]
        starts = self.__find_all_coords("a")
        end = self.__find_coords("E")
        all_lengths = []
        count = 0
        for i in starts:
            count += 1
            all_lengths.append(self.__routes_to_end(i, end, []))
        clean = [x for x in all_lengths if x is not None]
        clean.sort()
        return clean[0]

    def __routes_to_end(self, start, end, visited):
        queue = deque(((start, 0),))
        visited = []
        while queue:
            visiting, distance = queue.popleft()
            if visiting == [0, 2]:
                pass
            if visiting == end:
                return distance
            if visiting in visited:
                continue
            visited.append(visiting)
            coords_to_visit = [[visiting[0], visiting[1] - 1], [visiting[0], visiting[1] + 1],
                               [visiting[0] - 1, visiting[1]],
                               [visiting[0] + 1, visiting[1]]]
            for i in coords_to_visit:
                if len(self.grid) > i[0] >= 0 and len(self.grid[0]) > i[1] >= 0:
                    if ord(self.letter_at(i)) < ord(self.letter_at(visiting)) + 2:
                        if i not in visited:
                            queue.append((i, distance + 1))

    def letter_at(self, coords):
        letter = self.grid[coords[0]][coords[1]]
        if letter == 'S':
            return 'a'
        elif letter == 'E':
            return 'z'
        else:
            return letter

    # Returns the first occurrence of the specified symbol
    def __find_coords(self, symbol):
        for i in range(0, len(self.grid)):
            for j in range(0, len(self.grid[i])):
                if self.grid[i][j] == symbol:
                    return [i, j]

    # Returns all occurrences of the specified symbol
    def __find_all_coords(self, symbol):
        list = []
        for i in range(0, len(self.grid)):
            for j in range(0, len(self.grid[i])):
                if self.grid[i][j] == symbol:
                    list.append([i, j])
        return list