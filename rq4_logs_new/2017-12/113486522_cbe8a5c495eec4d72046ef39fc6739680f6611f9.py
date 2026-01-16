def split_garbage(text):
    garbage_free_text = ""
    garbage = ""
    currently_garbage = False
    ignore_next = False
    for char in text:
        if currently_garbage:
            if ignore_next:
                ignore_next = False
            elif char == "!":
                ignore_next = True
            elif char == ">":
                currently_garbage = False
            else:
                garbage += char
        else:
            if char == "<":
                currently_garbage = True
            else:
                garbage_free_text += char

    return garbage_free_text, garbage


def find_groups(text, position):
    groups = []
    position += 1

    while True:
        char = text[position]
        if char == "{":
            sub_group, position = find_groups(text, position)
            groups.append(sub_group)
        elif char == "}":
            return groups, position

        position += 1


def compute_scores(groups, parent_score):
    current_group_score = parent_score + 1
    return sum([current_group_score] + [compute_scores(group, current_group_score) for group in groups])


string_with_garbage = open("day_9_input", "r").read()

garbage_free_text, garbage = split_garbage(string_with_garbage)

print(
    "group scores :",
    compute_scores(
        find_groups(garbage_free_text, 0)[0],
        0
    )
)
print("garbage length :", len(garbage))