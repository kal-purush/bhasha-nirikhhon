import re

input_file = open("day_7_input", "r")
regex_for_line = re.compile("([a-z]+) \([0-9]+\)(?: -> ((?:[a-z]+, )*[a-z]+))?")


def parse_line(line, names, children):
    line = line.rstrip("\n")

    line_parsed = regex_for_line.match(line)
    node_name, node_children = line_parsed.group(1), line_parsed.group(2)

    names.add(node_name)
    if node_children:
        children.update(node_children.split(", "))

names = set()
children = set()
for line in input_file:
    parse_line(line, names, children)

nodes_that_are_not_children = names - children
assert len(nodes_that_are_not_children) == 1
print(nodes_that_are_not_children.pop())