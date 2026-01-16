'''Location Management: This program contains algorithms determining search and updates
                        for mobile station movements.

'''

__author__ = 'James Soehlke and David Taylor'
__version__ = '$Revision: 1.0 $'[11:-2]
__date__ = '$Date: 2016/02/06 12:00:00 $'
__copyright__ = 'Copyright (c) 2016 James Soehlke and David N. Taylor'
__license__ = 'Python'


POINTER_VALUE = "pointer"
ACTUAL_VALUE = "value"
FORWARDING_POINTER = "forwarding"
MOBILE_STATION = "mobile station"
LOCATION = "location"


class Tree:
    def __init__(self):
        self.__node_list = []
        self.__root_node = None

    def add_node(self,n):
        if self.__node_list.count(n) == 0:
            self.__node_list.append(n)
            if n.is_root():
                self.__root_node = n

    def get_root_node(self):
        return self.__root_node

    def get_node(self, name):
        if self.__root_node is not None:
            return self.__root_node.get_child_with_name(name)

        return None

    @staticmethod
    def get_lca(node1, node2):
        n1 = node1
        n2 = node2

        while n1 is not n2:
            while not n2.is_root():
                n2 = n2.get_parent_node()
                if n1 is n2:
                    return n2

            if n1.is_root():
                break
            else:
                n1 = n1.get_parent_node()
                n2 = node2

        if n1 is n2:
            return n2
        else:
            return None

    def put_ms_into_node_name(self, ms, name):
        node = self.get_node(name)
        if node is None:
            return False

        node.add_ms_and_actual_location(ms, node)
        node.set_ms_pointer_location(ms, node)
        ms.set_node(node)

        print node.get_name()

        while not node.is_root():
            node_parent = node.get_parent_node()
            node_parent.add_ms_and_actual_location(ms, ms.get_node())
            node_parent.set_ms_pointer_location(ms, node)
            node = node_parent
            print node.get_name()

        return True


class Node:
    def __init__(self, name):
        self.__name = name
        self.__parent_node = None
        self.__children_node = []
        self.__ms_list = {}

    def get_name(self):
        return self.__name

    def get_children_nodes(self):
        return self.__children_node

    def get_child_with_name(self, name):
        if self.get_name() == name:
            return self

        for i in range(self.__children_node.__len__()):
            n = self.__children_node[i].get_child_with_name(name)
            if n is not None:
                return n

        return None

    def add_child_node(self, node):
        if self.__children_node.count(node) == 0:
            self.__children_node.append(node)

    def add_parent_node(self, parent):
        self.__parent_node = parent

    def get_parent_node(self):
        return self.__parent_node

    def find_ms(self, ms, loc=None):
        it = iter(self.__ms_list)
        for i in it:
            if ms is self.__ms_list[i][MOBILE_STATION]:
                if loc is not None:
                    if self.__ms_list[i][LOCATION][loc] is not None:
                        return i
                else:
                    return i

        return None

    def get_new_ms_list_index(self):
        i = 0
        it = iter(self.__ms_list)
        for j in it:
            if i <= j:
                i = j + 1
        return i

    def add_ms(self, ms):
        if self.find_ms(ms) is None:
            l = self.get_new_ms_list_index()
            self.__ms_list[l] = {}
            self.__ms_list[l][MOBILE_STATION] = ms
            self.__ms_list[l][LOCATION] = {}
            self.__ms_list[l][LOCATION][POINTER_VALUE] = None
            self.__ms_list[l][LOCATION][ACTUAL_VALUE] = None
            self.__ms_list[l][LOCATION][FORWARDING_POINTER] = None
            print self.__ms_list.__len__()
            print self.__ms_list

    def contains_ms(self, ms):
        return self.find_ms(ms) is None

    def is_root(self):
        return self.__parent_node is None

    def is_leaf(self):
        return self.__children_node.__len__() == 0

    def add_ms_and_pointer_location(self, ms, loc):
        if self.find_ms(ms) is None:
            self.add_ms(ms)
            i = self.find_ms(ms)
            self.__ms_list[i][LOCATION][POINTER_VALUE] = loc
            print "add_ms_and_pointer_location"
            print self.__ms_list
            return True
        else:
            return self.set_ms_pointer_location(ms, loc)

    def add_ms_and_actual_location(self, ms, loc):
        if self.find_ms(ms) is None:
            self.add_ms(ms)
            i = self.find_ms(ms)
            self.__ms_list[i][LOCATION][ACTUAL_VALUE] = loc
            print "add_ms_and_actual_location"
            print self.__ms_list
            return True
        else:
            return self.set_ms_actual_location(ms, loc)

    def set_ms_pointer_location(self, ms, loc):
        i = self.find_ms(ms)
        if i is not None:
            self.__ms_list[i][LOCATION][POINTER_VALUE] = loc
            print "set_ms_pointer_location"
            print self.__ms_list
            return True

        return False

    def get_ms_pointer_location(self, ms):
        i = self.find_ms(ms)
        if i is not None:
            return self.__ms_list[i][LOCATION][POINTER_VALUE]

        return None

    def set_ms_actual_location(self, ms, loc):
        i = self.find_ms(ms)
        if i is not None:
            self.__ms_list[i][LOCATION][ACTUAL_VALUE] = loc
            return True

        return False

    def get_ms_actual_location(self, ms):
        i = self.find_ms(ms)
        if i is not None:
            return self.__ms_list[i][LOCATION][ACTUAL_VALUE]

        return None

    def delete_ms_pointer_location(self, ms):
        i = self.find_ms(ms)
        if i is not None:
            self.__ms_list[i][LOCATION][POINTER_VALUE] = None
            if self.__ms_list[i][LOCATION][ACTUAL_VALUE] is None:
                del self.__ms_list[i]
                print self.__ms_list

    def delete_ms_actual_location(self, ms):
        i = self.find_ms(ms)
        print self.__ms_list
        if i is not None:
            self.__ms_list[i][LOCATION][ACTUAL_VALUE] = None
            if self.__ms_list[i][LOCATION][POINTER_VALUE] is None:
                del self.__ms_list[i]
                print self.__ms_list


class MS:
    def __init__(self, name):
        self.__name = name
        self.__node = None

    def set_ms_name(self, name):
        self.__name = name

    def get_ms_name(self):
        return self.__name

    def set_node(self, node):
        self.__node = node

    def get_node(self):
        return self.__node

class Algorithm:
    def __init__(self, tree):
        self.__search_count = 0
        self.__update_count = 0
        self.__tree = tree

    def set_search_count(self, value):
        self.__search_count = value

    def get_search_count(self):
        return self.__search_count

    def increment_search_count(self):
        self.__search_count += 1

    def set_update_count(self, value):
        self.__update_count = value

    def get_update_count(self):
        return self.__update_count

    def increment_update_count(self):
        self.__update_count += 1

    def get_tree(self):
        return self.__tree

    def query_ms_location_from_node(self, ms, node):
        return

    def move_ms_to_node(self, ms, node):
        return


class ValueAlgorithm(Algorithm):
    def __init__(self, tree):
        Algorithm.__init__(self, tree)

    def find_node_and_query_ms_location_from_node(self, ms, name):
        node = self.get_tree().get_node(name)

        return self.query_ms_location_from_node(ms, node)

    def query_ms_location_from_node(self, ms, node):
        self.set_search_count(0)

        found = False

        if node is None:
            return node

        # check the node
        if node.find_ms(ms, ACTUAL_VALUE) is not None:
            found = True
            if node is node.get_ms_actual_location(ms):
                return node

        # look up the tree
        if not found:
            if node.is_root():
                return None

            found = False

            node = node.get_parent_node()
            self.increment_search_count()

            while (not found) and (not node.is_root()):
                if node.find_ms(ms, ACTUAL_VALUE) is not None:
                    found = True
                    if node is node.get_ms_actual_location(ms):
                        return node
                else:
                    node = node.get_parent_node()
                    self.increment_search_count()

        return None

    def find_node_and_move_ms_location_from_node(self, ms, name):
        node = self.get_tree().get_node(name)

        return self.move_ms_to_node(ms, node)

    def move_ms_to_node(self, ms, node):
        self.set_update_count(0)

        ms_node = ms.get_node()

        if ms_node is node:
            return

        lca = self.get_tree().get_lca(ms_node, node)

        while lca is not ms_node:
            ms_node.delete_ms_actual_location(ms)
            ms_node = ms_node.get_parent_node()
            self.increment_update_count()

        ms_node = node

        while ms_node is not None:
            ms_node_parent = ms_node.get_parent_node()
            if not ms_node.add_ms_and_actual_location(ms, node):
                ms_node.set_ms_actual_location(ms, node)
            ms_node = ms_node.get_parent_node()
            self.increment_update_count()

        ms.set_node(node)


class PointerAlgorithm(Algorithm):
    def __init__(self, tree):
        Algorithm.__init__(self, tree)

    def find_node_and_query_ms_location_from_node(self, ms, name):
        node = self.get_tree().get_node(name)

        return self.query_ms_location_from_node(ms, node)

    def query_ms_location_from_node(self, ms, node):
        self.set_search_count(0)

        found = False

        if node is None:
            return node

        # check the node
        if node.find_ms(ms, POINTER_VALUE) is not None:
            found = True
            if node is node.get_ms_pointer_location(ms):
                return node

        # look up the tree
        if not found:
            if node.is_root():
                return None

            found = False

            node = node.get_parent_node()
            self.increment_search_count()

            while (not found) and (not node.is_root()):
                if node.find_ms(ms, POINTER_VALUE) is not None:
                    found = True
                else:
                    node = node.get_parent_node()
                    self.increment_search_count()

            # Now look down the pointer list
            if found:
                if node is node.get_ms_pointer_location(ms):
                    return node

            while node is not node.get_ms_pointer_location(ms):
                node = node.get_ms_pointer_location(ms)
                self.increment_search_count()

            return node

        return None

    def find_node_and_move_ms_location_from_node(self, ms, name):
        node = self.get_tree().get_node(name)

        return self.move_ms_to_node(ms, node)

    def move_ms_to_node(self, ms, node):
        self.set_update_count(0)

        ms_node = ms.get_node()

        if ms_node is node:
            return

        lca = self.get_tree().get_lca(ms_node, node)

        while lca is not ms_node:
            ms_node.delete_ms_pointer_location(ms)
            ms_node = ms_node.get_parent_node()
            self.increment_update_count()

        ms_node = node

        while (lca is not ms_node) and (lca is not ms_node.get_parent_node()):
            ms_node_parent = ms_node.get_parent_node()
            if not ms_node_parent.add_ms_and_pointer_location(ms, ms_node):
                ms_node_parent.set_ms_pointer_location(ms, ms_node)
            ms_node = ms_node_parent
            self.increment_update_count()

        lca.set_ms_pointer_location(ms, ms_node)
        self.increment_update_count()
        if not ms_node.add_ms_and_pointer_location(ms, ms_node):
            ms_node.set_ms_pointer_location(ms, ms_node)
        self.increment_update_count()

        ms.set_node(node)


def fill_tree(tree):
    root_node = Node(0)
    tree.add_node(root_node)

    node_count = 0
    for i in range(2):
        node_count += 1
        inode = Node(node_count)
        inode.add_parent_node(root_node)
        root_node.add_child_node(inode)
        tree.add_node(inode)

        for j in range(3):
            node_count += 1
            jnode = Node(node_count)
            jnode.add_parent_node(inode)
            inode.add_child_node(jnode)
            tree.add_node(jnode)

            for k in range(2):
                node_count += 1
                knode = Node(node_count)
                knode.add_parent_node(jnode)
                jnode.add_child_node(knode)
                tree.add_node(knode)
    return


if __name__ == "__main__":
    t = Tree()
    fill_tree(t)

    ms1 = MS(1)

    t.put_ms_into_node_name(ms1, 6)

    ms2 = MS(2)

    t.put_ms_into_node_name(ms2, 17)
    pa = ValueAlgorithm(t)
    print pa.get_search_count()
    print pa.get_update_count()
    c = pa.find_node_and_query_ms_location_from_node(ms2, 20)
    print pa.get_update_count()
    print pa.get_search_count()
    pa.find_node_and_move_ms_location_from_node(ms1, 20)
    print pa.get_update_count()
    print pa.get_search_count()

