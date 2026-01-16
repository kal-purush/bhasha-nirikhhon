from util import iohandler

# --- solution ---


def get_adapter_arrangement_count(input_file):
    adapter_list = [int(row.strip()) for row in input_file]
    adapter_list.append(0)
    adapter_list.append(max(adapter_list) + 3)
    adapter_list.sort()
    combinations_graph = dict((adapter, []) for adapter in adapter_list)  # {child, [parents]}

    for adapter in adapter_list:
        for i in range(1, 4):
            if combinations_graph.get(adapter - i) is not None:
                combinations_graph.get(adapter).append(adapter - i)

    return len(find_all_paths(combinations_graph, max(adapter_list), 0))


# on the real input it's eating all the ram - find an algorithm
def find_all_paths(graph, start, end, path=[]):
    path = path + [start]
    if start == end:
        return [path]

    if start not in graph:
        return []

    paths = []

    for node in graph[start]:
        if node not in path:
            newpaths = find_all_paths(graph, node, end, path)
            for newpath in newpaths:
                paths.append(newpath)

    return paths


# --- solution ---

if __name__ == '__main__':
    iohandler.end(str(get_adapter_arrangement_count(iohandler.begin(__file__))))