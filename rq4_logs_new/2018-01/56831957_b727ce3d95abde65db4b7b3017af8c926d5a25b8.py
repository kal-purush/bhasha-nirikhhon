class FloydWarshal(object):
    """
    Goal, to find shortest path for all pair of vertices and
    to find if there is a negative weight cycle (which also
    means this algorithm can work with negative weight edges)
    """

    def __init__(self, size):
        """
        Note: distances are inf and discovered is None
        """
        self.size = size
        self.distance = [[float('inf') for _ in range(size)] for _ in range(size)]
        self.discovered = [[None for _ in range(size)] for _ in range(size)]

    def populate(self, graph):
        """
        Initiate distance and discovered for directly
        connected vertices

        Note: You also need to set discovered[i][i] to 0,
        i.e. distance from vertex i to vertex i is 0
        """
        for k, v in graph.items():
            for sk, sv in v.items():
                self.distance[int(k)][int(sk)] = sv
                self.discovered[int(k)][int(sk)] = k

        # TODO: This is important!!
        for i in range(self.size):
            self.distance[i][i] = 0

    def floyd_warshal(self, graph):
        """
        Its all about finding a path from i to j through k.
        If there is such a distance (that is less then the current
        distance from i to j) then update that distance.

        As for the discovered matrix, If there is a shorter path from
        i to j via k then update the path from i to j to path from k to j
        (basically saying you discovered a path from i to j via k)

        To check for negative weight cycle, check for the diagonal values
        in the distance matrix, if any of the values is negative then
        there is a negative weight cycle

        NOTE: The ordering of the for loops (its:  k, i, j)
        """
        self.populate(graph)
        print self.discovered
        print self.distance
        print ''

        for k in range(self.size):
            for i in range(self.size):
                for j in range(self.size):
                    if self.distance[i][j] > self.distance[i][k] + self.distance[k][j]:
                        self.distance[i][j] = self.distance[i][k] + self.distance[k][j]
                        self.discovered[i][j] = self.discovered[k][j]

        print self.discovered
        print self.distance

        for i in range(self.size):
            if self.distance[i][i] < 0:
                print "There is a negative weight cycle"


if __name__ == '__main__':
    graph = {
        '0': {
            '1': 3,
            '2': 6,
            '3': 15
        },
        '1': {
            '2': -2
        },
        '2': {
            '3': 2
        },
        '3': {
            '0': 1
        }
    }

    fw = FloydWarshal(len(graph))
    fw.floyd_warshal(graph)