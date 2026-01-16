import string


def parse(line):
    s = line.strip().split()
    before = s[1]
    after = s[7]
    return before, after


def part1(data):
    deps = []
    for line in data:
        x, y = parse(line)  # x -> y edge
        deps.append((x, y))

    steps = set([x[0] for x in deps] + [x[1] for x in deps])
    steps = sorted(list(steps))

    path = ""

    print(deps)

    i = 0
    while len(steps) > 0:
        s = steps[i]

        has_dep = False
        ind2delete = []

        for j, d in enumerate(deps):
            if s == d[0]:
                ind2delete.append(j)

            if s == d[1]:
                has_dep = True

        if has_dep:
            i += 1
        else:
            path += str(steps.pop(i))
            deps = [d for ind, d in enumerate(deps) if ind not in ind2delete]

            i = 0

    print("Path:", path)
    # print(steps)
    # print(deps)


def dfs(graph, root):
    nodes = []
    queue = [root]

    while len(queue) > 0:
        n = queue.pop(0)
        if n not in nodes:
            nodes.append(n)

        if n in graph:
            children = sorted(graph[n])
        else:
            children = []
        queue.extend(children)

    # print(nodes)
    return nodes


def get_time(c):
    return string.ascii_uppercase.index(c) + 1


def part2(data):
    graph = {}
    root = None

    for line in data:
        x, y = parse(line)

        if root is None:
            root = x

        if x in graph:
            graph[x].append(y)
        else:
            graph[x] = [y]

    time = -1
    workers = [0, ] * 5

    jobs = dfs(graph, root)
    workers[0] = get_time(jobs.pop())

    print(workers)

    while True:
        time += 1
        for idx in range(len(workers)):
            if workers[idx] > 0:
                workers[idx] -= 1

        for i, w in enumerate(workers):
            if w == 0:
                workers[i] = get_time(jobs.pop())
        print(time, workers)
        if sum(workers) == 0:
            break


data = ["Step C must be finished before step A can begin.",
        "Step C must be finished before step F can begin.",
        "Step A must be finished before step B can begin.",
        "Step A must be finished before step D can begin.",
        "Step B must be finished before step E can begin.",
        "Step D must be finished before step E can begin.",
        "Step F must be finished before step E can begin."]

# with open('day7.txt') as f:
#     data = f.readlines()

part1(data)
part2(data)