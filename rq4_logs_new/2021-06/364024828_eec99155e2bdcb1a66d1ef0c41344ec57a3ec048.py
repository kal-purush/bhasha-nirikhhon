from Graph import Graph


class AdjacencyListDiGraph(Graph):
    def __init__(self):
        super().__init__()
        self._adjacency_list = {}

    def weight(self, source, destination):
        if destination not in self._adjacency_list[source].keys():
            raise ValueError
        return self._adjacency_list[source][destination]

    def add_node(self, *args) -> None:
        for node in args:
            if node in self._adjacency_list.keys():
                raise ValueError
            self._adjacency_list[node] = self._adjacency_list.get(node, {})
            self._number_of_nodes += 1

    def nodes(self):
        nodes_iterator = iter(self._adjacency_list.keys())
        try:
            while True:
                next_node = next(nodes_iterator)
                yield next_node
        except StopIteration:
            pass

    def add_edge(self, *args: tuple) -> None:
        for edge in args:
            if len(edge) > 3 or len(edge) < 2 or edge[0] not in self._adjacency_list.keys() or \
                    edge[1] not in self._adjacency_list.keys():
                raise ValueError
            source, destination = edge[0], edge[1]
            if destination in self._adjacency_list[source].keys():  # raise error if edge already exists
                raise ValueError
            if len(edge) == 3:
                if edge[2] == 0:
                    raise ValueError
                weight = edge[2]
            else:
                weight = 1
            self._adjacency_list[source][destination] = self._adjacency_list[source].get(source, weight)
            self._number_of_edges += 1

    def edges(self):
        for vertex, neighborhood in self._adjacency_list.items():
            for neighbor in neighborhood:
                yield vertex, neighbor

    def neighbors(self, node):
        for vertex, neighborhood in self._adjacency_list.items():
            if vertex == node:
                for neighbor in self._adjacency_list[vertex]:
                    yield neighbor
            else:
                if vertex not in self._adjacency_list[node].keys() and node in self._adjacency_list[vertex]:
                    yield vertex

    def ingoing_neighbors(self, node):
        for vertex, neighborhood in self._adjacency_list.items():
            if vertex != node:
                for neighbor in neighborhood:
                    if neighbor == node:
                        yield vertex

    def outgoing_neighbors(self, node):
        neighbors_iterator = iter(self._adjacency_list[node])
        try:
            while True:
                neighbor = next(neighbors_iterator)
                yield neighbor
        except StopIteration:
            pass
