class Career:
    def __init__(self, filename):
        self.init_vertex = ''
        self.graph = self.reader(filename)
        self.score = 0

    def find_optimal_path(self, init_vertex=None, path='', score=0):
        if init_vertex is None:
            init_vertex = self.init_vertex
        new_path = path + init_vertex + '->'
        print(new_path[:-2], score, sep=' | ')
        highest_score = 0
        if self.graph[init_vertex]:
            for next_vertex in self.graph[init_vertex]:
                if next_vertex not in new_path.split('|'):
                    new_score = self.find_optimal_path(next_vertex, new_path, score + self.graph[init_vertex][next_vertex])
                    if new_score > highest_score:
                        highest_score = new_score
            return highest_score
        else:
            return score

    def reader(self, filename):
        f = open(filename)
        num_lines = int(f.readline())
        graph = dict()
        values = [[] for _ in range(num_lines)]
        item = 1  # We will need to keep count of each new vertex
        graph[str(item)] = {}
        for i in range(num_lines):
            line = list(map(int, f.readline().split(" ")))
            for value in line:
                values[i].append({str(item): value})
                item += 1
                graph[str(item)] = {}
        values.append([{str(item): 0}])  # Additional vertex to start from
        values.reverse()
        self.init_vertex = str(item)
        for element in values[1]:
            graph[str(item)].update(element)
        for i in range(1, len(values)):
            for j in range(len(values[i]) - 1, -1, -1):
                item -= 1
                try:
                    graph[str(item)].update(values[i + 1][j])
                except IndexError:
                    pass
                try:
                    if j == 0:
                        break
                    graph[str(item)].update(values[i + 1][j - 1])
                except IndexError:
                    pass
        f.close()
        return graph