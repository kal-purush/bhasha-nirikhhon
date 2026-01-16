from util import iohandler

# --- solution ---


def get_multiplied_joltage_differences(input_file):
    diff_one = 1  # outlet diff +1 - (it's incorrect if first adapter is not 1, but i did not see mentioning for it)
    diff_tree = 1  # device diff +1
    adapter_list = sorted([int(row.strip()) for row in input_file])

    for index in range(len(adapter_list) - 1):
        if adapter_list[index+1] - adapter_list[index] == 1:
            diff_one += 1
        else:
            diff_tree += 1

    return diff_one * diff_tree


# --- solution ---

if __name__ == '__main__':
    iohandler.end(str(get_multiplied_joltage_differences(iohandler.begin(__file__))))