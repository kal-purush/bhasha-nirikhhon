from util import iohandler

# --- solution ---

right_step_num = 3


def init(input_file):
    tree_map = list()
    for row in input_file:
        tree_map.append(str(row.strip()))
    return tree_map


def sum_of_trees_in_the_slope(tree_map):
    field_max = len(tree_map[0])
    field_current = 3
    tree_counter = 0
    for row_num in range(len(tree_map)):
        if row_num == 0:
            continue

        if tree_map[row_num][field_current] == '#':
            tree_counter += 1

        step_to = field_current + right_step_num
        if step_to > field_max - 1:
            field_current = step_to - field_max
        else:
            field_current = step_to

    return tree_counter


# --- solution ---

if __name__ == '__main__':
    input_list = init(iohandler.begin(__file__))
    iohandler.end(str(sum_of_trees_in_the_slope(input_list)))