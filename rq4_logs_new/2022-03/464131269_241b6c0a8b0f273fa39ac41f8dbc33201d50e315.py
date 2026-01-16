class Child:
    def __init__(self):
        self.__up = -1
        self.__down = -1
        self.__nextLIndex = -1

    def set_up(self, up: int):
        self.__up = up

    def set_down(self, down: int):
        self.__down = down

    def set_next_l_index(self, nextLIndex: int):
        self.__nextLIndex = nextLIndex

    def get_up(self):
        return self.__up

    def get_down(self):
        return self.__down

    def get_next_l_index(self):
        return self.__nextLIndex

    def __str__(self):
        return f"up=%s, down=%s, nextLIndex=%s" % (self.__up, self.__down, self.__nextLIndex)


def compute_up_and_down_values(child_table, lcp_table):
    last_idx = -1
    stack: list[int] = [0]

    for i in range(1, len(lcp_table)):
        top = stack[len(stack) - 1]

        while lcp_table[i] < lcp_table[top]:
            last_idx = stack.pop()
            top = stack[len(stack) - 1]

            if (lcp_table[i] <= lcp_table[top]) and (lcp_table[top] != lcp_table[last_idx]):
                child_table[top].set_down(last_idx)

        if last_idx != -1:
            child_table[i].set_up(last_idx)
            last_idx = -1

        stack.append(i)


def compute_next_l_indices(child_table, lcp_tab):
    stack: list[int] = [0]

    for i in range(1, len(lcp_tab)):
        top = stack[len(stack) - 1]

        while lcp_tab[i] < lcp_tab[top]:
            stack.pop()
            top = stack[len(stack) - 1]

        if lcp_tab[i] == lcp_tab[top]:
            last_idx = stack.pop()
            child_table[last_idx].set_next_l_index(i)

        stack.append(i)


def compute_child_table(suf_tab, lcp_tab):
    child_table: list[Child] = []

    for i in range(0, len(lcp_tab)):
        child_table.append(Child())

    compute_up_and_down_values(child_table, lcp_tab)
    compute_next_l_indices(child_table, lcp_tab)

    for i in range(0, len(child_table)):
        print(child_table[i])

    return child_table


def get_child_intervals(i, j, child_tab: list[Child]):
    intervals = []

    if i == 0 and j == len(child_tab):
        return intervals

    i_1: int = -1
    i_2: int = -1

    if i < child_tab[j + 1].get_up() <= j:
        i_1 = child_tab[j + 1].get_up()
    else:
        i_1 = child_tab[i].get_down()

    intervals.append((i, i_1))

    while child_tab[i_1].get_next_l_index() != -1:
        i_2 = child_tab[i_1].get_next_l_index()

        intervals.append((i_1, i_2 - 1))

        i_1 = i_2

    intervals.append((i_1, j))

    return intervals


def get_lcp(i, j, child_table: list[Child], lcp_tab):
    if (i < child_table[j + 1].get_up()) and (child_table[j + 1].get_up() <= j):
        return lcp_tab[child_table[j + 1].get_up()]
    else:
        return lcp_tab[child_table[i].get_down()]


def perform_top_down_traversal(s, p, suf_tap, lcp_tab):
    child_table = compute_child_table(suf_tap, lcp_tab)
    intervals: list[(int, int)] = get_child_intervals(1, 3, child_table)