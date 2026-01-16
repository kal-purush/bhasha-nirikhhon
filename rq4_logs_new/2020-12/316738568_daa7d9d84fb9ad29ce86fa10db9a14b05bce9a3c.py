from util import iohandler
#from anytree import Node, RenderTree, Walker
from treelib import Node, Tree

# --- solution ---

bag_dictionary = dict()
bag_tree = list()
shiny_nodes = set()
#w = Walker()


def init(input_file):
    return [str(row.strip()) for row in input_file]


def containing_bag_num(input_rules):
    build_dict(input_rules)
    #bag_tree.append(Node('luggages'))
    #top_bags = search_parents()
    layers = 0

    print(bag_dictionary)

    added = set()
    tree = Tree()
    while bag_dictionary:
        for key, value in bag_dictionary.items():
            for val in value:
                tree.create_node(val, val, parent=key)
            added.add(key)
            bag_dictionary.pop(key)
            break

    tree.show()

    """for par in top_bags:
        #print(par)
        build_tree(par, bag_dictionary.get(par.name))

    for shiny_node in shiny_nodes:
        #print(str(w.walk(bag_tree[0], shiny_node)))
        layers += ''.join(list(str(w.walk(bag_tree[0], shiny_node)).split(',')[1:])).count('Node') - 1

    #print_tree()
    """
    return layers


def build_dict(input_rules):
    for rule in input_rules:
        splitted = rule.split(' ')
        container = splitted[0] + " " + splitted[1]

        if bag_dictionary.get(container) is None:
            bag_dictionary.update({container: []})

        if len(splitted) > 7:
            for index in list(range(len(splitted)))[4::4]:
                bag_dictionary.get(container).append(splitted[index + 1] + " " + splitted[index + 2])


"""def build_tree(parent, childs):
    for bag in childs:
        bag_node = Node(bag, parent=parent)
        bag_tree.append(bag_node)

        if bag == 'shiny gold':
            shiny_nodes.add(bag_node.parent)
            print(bag_node.parent)

        if bag_dictionary.get(bag) is not None:
            build_tree(bag_node, bag_dictionary.pop(bag))


def search_parents():
    parents = set()
    parent_nodes = set()
    for rec in bag_dictionary:
        contained = False
        for val in bag_dictionary.values():
            if rec in val:
                contained = True

        if contained is not True:
            parents.add(rec)

    for parent in parents:
        bag_node = Node(parent, parent=bag_tree[0])
        parent_nodes.add(bag_node)
        bag_tree.append(bag_node)

    return parent_nodes


def print_tree():
    for pre, fill, node in RenderTree(bag_tree[0]):
        print("%s%s" % (pre, node.name))
"""

# --- solution ---

if __name__ == '__main__':
    input_list = init(iohandler.begin(__file__))
    iohandler.end(str(containing_bag_num(input_list)))