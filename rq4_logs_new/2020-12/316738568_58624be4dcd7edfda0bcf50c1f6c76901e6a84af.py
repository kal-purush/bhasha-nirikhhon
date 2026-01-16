from util import iohandler

# --- solution ---


def init(input_file):
    tree_map = list()
    for row in input_file:
        tree_map.append(str(row.strip()))
    return tree_map


def multiplied_tree_probabilities(tree_map):
    multiplied_tree_count = 1
    patterns = [[1, 1], [3, 1], [5, 1], [7, 1], [1, 2]]  # [right, down]
    for pattern in patterns:
        multiplied_tree_count *= count_trees_by_pattern(tree_map, pattern[0], pattern[1])

    return multiplied_tree_count


def count_trees_by_pattern(tree_map, right, down):
    field_max = len(tree_map[0])
    field_current = right
    tree_counter = 0
    for row_num in list(range(len(tree_map)))[0::down]:
        if row_num == 0:
            continue

        if tree_map[row_num][field_current] == '#':
            tree_counter += 1

        step_to = field_current + right
        if step_to > field_max - 1:
            field_current = step_to - field_max
        else:
            field_current = step_to

    return tree_counter


# --- solution ---

if __name__ == '__main__':
    input_list = init(iohandler.begin(__file__))
    iohandler.end(str(multiplied_tree_probabilities(input_list)))