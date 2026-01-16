'''Location Management Misc:
        This contains the MS, Tree, and Node classes for the algorithms.

'''

__author__ = 'James Soehlke and David Taylor'
__version__ = '$Revision: 1.0 $'[11:-2]
__date__ = '$Date: 2016/02/06 12:00:00 $'
__copyright__ = 'Copyright (c) 2016 James Soehlke and David N. Taylor'
__license__ = 'Python'

import os
import subprocess
from LocationManagementAlgorithms import *

POINTER = "pointer"
VALUE = "value"
FORWARDING_P = "forwarding_p"
FORWARDING_V = "forwarding_v"
FORWARDING_P_FORWARD = "forwarding_p_forward"
FORWARDING_P_REVERSE = "forwarding_p_reverse"
MOBILE_STATION = "mobile station"
LOCATION = "location"
REPLICATION = "replication"
REPLICATION_P = "replication_p"
REPLICATION_V = "replication_v"

algorithm_list = [POINTER, VALUE, FORWARDING_P, FORWARDING_V, REPLICATION_P, REPLICATION_V]
location_list = [FORWARDING_P_FORWARD, FORWARDING_P_REVERSE, REPLICATION]
location_list[len(location_list):] = algorithm_list


class Tree(object):
    def __init__(self, algorithm):
        self.__root_node = None
        self.__leaf_nodes = []
        self.__algorithm = algorithm
        if self.__algorithm is not None:
            self.__algorithm.set_tree(self)
        self.__node_update_list = []
        self.__node_search_list = []
        self.__tree_draw_count = 0
        return

    def add_node(self, node):
        if node.is_root():
            self.__root_node = node
        elif node.is_leaf():
            self.__leaf_nodes.append(node)
        return

    def get_root_node(self):
        return self.__root_node

    def get_leaf_nodes(self):
        return self.__leaf_nodes

    def get_node(self, name):
        if self.__root_node is not None:
            return self.__root_node.get_child_with_name(name)

        return None

    @staticmethod
    def get_lca(node1, node2):
        n1 = node1
        n2 = node2

        if (n1 is None) or (n2 is None):
            return None

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

        if (self.__algorithm.get_type() == VALUE) or (self.__algorithm.get_type() == FORWARDING_V) or (self.__algorithm.get_type() == REPLICATION_V):
            node.add_ms_location(ms, node, self.__algorithm.get_type())
        else:
            node.add_ms_location(ms, node, self.__algorithm.get_type())
        ms.set_node(node, self.__algorithm.get_type())

        #print node.get_name()

        while not node.is_root():
            node_parent = node.get_parent_node()
            if (self.__algorithm.get_type() == VALUE) or (self.__algorithm.get_type() == FORWARDING_V) or (self.__algorithm.get_type() == REPLICATION_V):
                node_parent.add_ms_location(ms, ms.get_node(self.__algorithm.get_type()), self.__algorithm.get_type())
            else:
                node_parent.add_ms_location(ms, node, self.__algorithm.get_type())
            node = node_parent
            #print node.get_name()

        return True

    def get_update_count(self):
        return self.__algorithm.get_update_count()

    def get_search_count(self):
        return self.__algorithm.get_search_count()

    def find_node_and_query_ms_location_from_node(self, ms, name):
        del self.__node_search_list[:]
        return self.__algorithm.find_node_and_query_ms_location_from_node(ms, name)

    def query_ms_location_from_node(self, ms, node):
        del self.__node_search_list[:]
        return self.__algorithm.query_ms_location_from_node(ms, node)

    def find_node_and_move_ms_location_from_node(self, ms, name):
        del self.__node_update_list[:]
        return self.__algorithm.find_node_and_move_ms_location_from_node(ms, name)

    def move_ms_to_node(self, ms, node):
        del self.__node_update_list[:]
        return self.__algorithm.move_ms_to_node(ms, node)

    def get_algorithm(self):
        return self.__algorithm

    def add_node_search(self, node):
        if self.__node_search_list.count(node) == 0:
            self.__node_search_list.append(node)

    def add_node_update(self, node):
        if not self.has_node_been_updated(node):
            self.__node_update_list.append(node)

    def has_node_been_updated(self, node):
        return self.__node_update_list.count(node) > 0

    def draw_tree(self, ms1, ms2):
        self.__tree_draw_count += 1
        graphtitle = self.get_algorithm().get_name() + "-" + str(self.__tree_draw_count)
        dotfilename = self.get_algorithm().get_name() + "-" + str(self.__tree_draw_count) + ".dot"
        pngfilename = self.get_algorithm().get_name() + "-" + str(self.__tree_draw_count) + ".png"
        oscommand = "dot -Tpng " + dotfilename + " > " + pngfilename

        f = open(dotfilename, 'w')
        text = "strict graph G{label=\"" + graphtitle + "\\n"
        text = text + "Search = " + str(self.get_search_count()) + ", "
        text = text + "Update = " + str(self.get_update_count()) + "\""
        f.write(text)
        f.write("\n")
        self.draw_nodes(f, self.__root_node, ms1, ms2)
        self.draw_edges(f, self.__root_node)
        self.draw_search_edges(f, self.__node_search_list)
        f.write('}')
        f.close()

        print subprocess.call(oscommand, shell=True)
        print subprocess.call(pngfilename, shell=True)

        return

    def draw_nodes(self, f, node, ms1, ms2):
        it = node.get_children_nodes()
        for i in it:
            self.draw_nodes(f, i, ms1, ms2)
        self.draw_node(f, node, ms1, ms2)

    def draw_node(self, f, node, ms1, ms2):
        text = str(node.get_name()) + "[label=\"" + str(node.get_name())
        if ms1.get_node(self.get_algorithm().get_type()) is node:
            text = text + "\\nMS" + str(ms1.get_name())
        elif ms2.get_node(self.get_algorithm().get_type()) is node:
            text = text + "\\nMS" + str(ms2.get_name())
        text = text + "\""
        if self.__node_update_list.count(node) > 0:
            text = text + ",color = lightblue,style = filled"
        text = text + "];"
        f.write(text)
        f.write("\n")

    def draw_edges(self, f, node):
        it = node.get_children_nodes()
        for i in it:
            self.draw_edges(f, i)

        if not node.is_root():
            self.draw_edge(f, node)

    def draw_edge(self, f, node):
        #0 -- 1;
        text = str(node.get_parent_node().get_name()) + " -- " + str(node.get_name()) + ";"
        f.write(text)
        f.write("\n")

    def draw_search_edges(self, f, search_list):
        self.get_algorithm().print_dot_search(f, search_list)


class Node(object):
    def __init__(self, name):
        self.__name = name
        self.__parent_node = None
        self.__children_node = []
        self.__ms_list = {}
        self.__lcmr = 0.0
        return

    def get_name(self):
        return self.__name

    def get_children_nodes(self):
        return self.__children_node

    def get_child_with_name(self, name):
        if self.get_name() == name:
            return self

        for i in range(len(self.__children_node)):
            n = self.__children_node[i].get_child_with_name(name)
            if n is not None:
                return n

        return None

    def add_child_node(self, node):
        if self.__children_node.count(node) == 0:
            self.__children_node.append(node)
        return

    def add_parent_node(self, parent):
        self.__parent_node = parent
        return

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
            it = iter(location_list)
            for loc in it:
                self.__ms_list[l][LOCATION][loc] = None
            #print self.__ms_list.__len__()
            #print self.__ms_list
        return

    def contains_ms(self, ms):
        return self.find_ms(ms) is None

    def is_root(self):
        return self.__parent_node is None

    def is_leaf(self):
        return len(self.__children_node) == 0

    def add_ms_location(self, ms, loc, loc_type):
        if self.find_ms(ms) is None:
            self.add_ms(ms)
            i = self.find_ms(ms)
            self.__ms_list[i][LOCATION][loc_type] = loc
            #print self.__ms_list
            return True
        else:
            return self.set_ms_location(ms, loc, loc_type)

    def set_ms_location(self, ms, loc, loc_type):
        i = self.find_ms(ms)
        if i is not None:
            self.__ms_list[i][LOCATION][loc_type] = loc
            #print self.__ms_list
            return True

        return False

    def get_ms_location(self, ms, loc_type):
        i = self.find_ms(ms)
        if i is not None:
            return self.__ms_list[i][LOCATION][loc_type]

        return None

    def delete_ms_location(self, ms, loc_type):
        i = self.find_ms(ms)
        if i is not None:
            self.__ms_list[i][LOCATION][loc_type] = None
            empty = True
            it = iter(location_list)
            for loc in it:
                if self.__ms_list[i][LOCATION][loc] is not None:
                    empty = False
            if empty:
                del self.__ms_list[i]
                #print self.__ms_list
                return True
            else:
                return False
        return True

    def delete_all_ms_locations(self, ms):
        i = self.find_ms(ms)
        if i is not None:
            del self.__ms_list[i]
            #print self.__ms_list
        return

    def set_lcmr(self, lcmr):
        self.__lcmr = lcmr
        return

    def get_lcmr(self):
        return self.__lcmr

    def calculate_lcmr(self):
        if self.is_leaf():
            return self.__lcmr

        self.__lcmr = 0

        it = iter(self.__children_node)
        for i in it:
            self.__lcmr += i.calculate_lcmr()

        return self.__lcmr



class MS(object):
    def __init__(self, name, max_forwards=5):
        self.__name = name
        self.__node_list = {}
        self.__max_forwards = max_forwards
        it = iter(algorithm_list)
        for node_type in it:
            self.__node_list[node_type] = None
        return

    def set_name(self, name):
        self.__name = name
        return

    def get_name(self):
        return self.__name

    def set_node(self, node, loc_type):
        self.__node_list[loc_type] = node
        return

    def get_node(self, loc_type):
        return self.__node_list[loc_type]

    def get_max_forwards(self):
        return self.__max_forwards


def fill_tree(tree):
    node0 = Node(0)
    node1 = Node(1)
    node2 = Node(2)
    node3 = Node(3)
    node4 = Node(4)
    node5 = Node(5)
    node6 = Node(6)
    node7 = Node(7)
    node8 = Node(8)
    node9 = Node(9)
    node10 = Node(10)
    node11= Node(11)
    node12 = Node(12)
    node13 = Node(13)
    node14 = Node(14)
    node15 = Node(15)
    node16 = Node(16)
    node17 = Node(17)
    node18 = Node(18)

    node7.set_lcmr(.2)
    node8.set_lcmr(.2)
    node9.set_lcmr(.2)
    node10.set_lcmr(.2)
    node11.set_lcmr(.2)
    node12.set_lcmr(.2)
    node13.set_lcmr(.2)
    node14.set_lcmr(.2)
    node15.set_lcmr(.2)
    node16.set_lcmr(.2)
    node17.set_lcmr(.2)
    node18.set_lcmr(.2)

    node0.add_child_node(node1)
    node0.add_child_node(node2)
    node1.add_parent_node(node0)
    node2.add_parent_node(node0)

    node1.add_child_node(node3)
    node1.add_child_node(node4)
    node3.add_parent_node(node1)
    node4.add_parent_node(node1)

    node2.add_child_node(node5)
    node2.add_child_node(node6)
    node5.add_parent_node(node2)
    node6.add_parent_node(node2)

    node3.add_child_node(node7)
    node3.add_child_node(node8)
    node3.add_child_node(node9)
    node7.add_parent_node(node3)
    node8.add_parent_node(node3)
    node9.add_parent_node(node3)

    node4.add_child_node(node10)
    node4.add_child_node(node11)
    node4.add_child_node(node12)
    node10.add_parent_node(node4)
    node11.add_parent_node(node4)
    node12.add_parent_node(node4)

    node5.add_child_node(node13)
    node5.add_child_node(node14)
    node5.add_child_node(node15)
    node13.add_parent_node(node5)
    node14.add_parent_node(node5)
    node15.add_parent_node(node5)

    node6.add_child_node(node16)
    node6.add_child_node(node17)
    node6.add_child_node(node18)
    node16.add_parent_node(node6)
    node17.add_parent_node(node6)
    node18.add_parent_node(node6)

    tree.add_node(node0)
    for i in range(18):
        tree.add_node(tree.get_node(i))

    return


if __name__ == "__main__":
    tpa = Tree(PointerAlgorithm())
    tfppa = Tree(ForwardingPointerPAlgorithm())
    tva = Tree(ValueAlgorithm())
    tfpva = Tree(ForwardingPointerVAlgorithm())
    trva = Tree(ReplicationValueAlgorithm())
    trpa = Tree(ReplicationPointerAlgorithm())
    fill_tree(tpa)
    fill_tree(tva)
    fill_tree(tfppa)
    fill_tree(tfpva)
    fill_tree(trva)
    fill_tree(trpa)

    ms1 = MS(1)

    tpa.put_ms_into_node_name(ms1, 7)
    tva.put_ms_into_node_name(ms1, 7)
    tfppa.put_ms_into_node_name(ms1, 7)
    tfpva.put_ms_into_node_name(ms1, 7)
    trpa.put_ms_into_node_name(ms1, 7)
    trva.put_ms_into_node_name(ms1, 7)

    ms2 = MS(2)

    tpa.put_ms_into_node_name(ms2, 18)
    tva.put_ms_into_node_name(ms2, 18)
    tfppa.put_ms_into_node_name(ms2, 18)
    tfpva.put_ms_into_node_name(ms2, 18)
    trpa.put_ms_into_node_name(ms2, 18)
    trva.put_ms_into_node_name(ms2, 18)

    #tpa.find_node_and_move_ms_location_from_node(ms1, 13)
    #print "PA Update Count  = %d" % tpa.get_update_count()
    tpa.find_node_and_move_ms_location_from_node(ms1, 13)
    p = tpa.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(POINTER).get_name())
    #tva.find_node_and_move_ms_location_from_node(ms1, 13)
    #print "VA Update Count  = %d" % tva.get_update_count()
    tva.find_node_and_move_ms_location_from_node(ms1, 13)
    v = tva.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(VALUE).get_name())
    #tfppa.find_node_and_move_ms_location_from_node(ms1, 13)
    #print "FPP Update Count = %d" % tfppa.get_update_count()
    tfppa.find_node_and_move_ms_location_from_node(ms1, 11)
    fpp = tfppa.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(FORWARDING_P).get_name())
    print "FPP Search Count = %d" % tfppa.get_search_count()
    print "FPP Update Count = %d" % tfppa.get_update_count()
    tfppa.find_node_and_move_ms_location_from_node(ms1, 13)
    fpp = tfppa.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(FORWARDING_P).get_name())
    print "FPP Search Count = %d" % tfppa.get_search_count()
    print "FPP Update Count = %d" % tfppa.get_update_count()
    tfppa.find_node_and_move_ms_location_from_node(ms1, 15)
    fpp = tfppa.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(FORWARDING_P).get_name())
    print "FPP Search Count = %d" % tfppa.get_search_count()
    print "FPP Update Count = %d" % tfppa.get_update_count()
    tfppa.find_node_and_move_ms_location_from_node(ms1, 13)
    fpp = tfppa.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(FORWARDING_P).get_name())
    #tfpva.find_node_and_move_ms_location_from_node(ms1, 13)
    #print "FPV Update Count = %d" % tfpva.get_update_count()
    #tfpva.find_node_and_move_ms_location_from_node(ms1, 11)
    #fpv = tfpva.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(FORWARDING_V).get_name())
    #print "FPV Search Count = %d" % tfpva.get_search_count()
    #print "FPV Update Count = %d" % tfpva.get_update_count()
    #tfpva.find_node_and_move_ms_location_from_node(ms1, 13)
    #fpv = tfpva.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(FORWARDING_V).get_name())
    #print "FPV Search Count = %d" % tfpva.get_search_count()
    #print "FPV Update Count = %d" % tfpva.get_update_count()
    #tfpva.find_node_and_move_ms_location_from_node(ms1, 12)
    #fpv = tfpva.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(FORWARDING_V).get_name())
    #print "FPV Search Count = %d" % tfpva.get_search_count()
    #print "FPV Update Count = %d" % tfpva.get_update_count()
    tfpva.find_node_and_move_ms_location_from_node(ms1, 13)
    fpv = tfpva.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(FORWARDING_V).get_name())
    trpa.find_node_and_move_ms_location_from_node(ms1, 13)
    p = trpa.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(POINTER).get_name())
    trva.find_node_and_move_ms_location_from_node(ms1, 13)
    p = trva.find_node_and_query_ms_location_from_node(ms1, ms2.get_node(POINTER).get_name())
    print
    print
    print "PA Search Count  = %d" % tpa.get_search_count()
    print "PA Update Count  = %d" % tpa.get_update_count()
    print "VA Search Count  = %d" % tva.get_search_count()
    print "VA Update Count  = %d" % tva.get_update_count()
    print "FPP Search Count = %d" % tfppa.get_search_count()
    print "FPP Update Count = %d" % tfppa.get_update_count()
    print "FPV Search Count = %d" % tfpva.get_search_count()
    print "FPV Update Count = %d" % tfpva.get_update_count()
    print "RPA Search Count  = %d" % trpa.get_search_count()
    print "RPA Update Count  = %d" % trpa.get_update_count()
    print "RVA Search Count  = %d" % trva.get_search_count()
    print "RVA Update Count  = %d" % trva.get_update_count()
    print
    print

    if (p.get_name() == v.get_name()) and (p.get_name() == fpp.get_name()) and (p.get_name() == fpv.get_name()):
        print "All search results are the same."
    else:
        print "All search results are NOT the same!!"
        print "p    = %d" % p.get_name()
        print "v    = %d" % v.get_name()
        print "fpp  = %d" % fpp.get_name()
        print "fpv  = %d" % fpv.get_name()

    #os.system("dot -Tpng pointer.dot > pointer.png")
    #os.system("pointer.png")
    #os.system("dot -Tpng pointer.dot > pointer1.png")
    #os.system("pointer1.png")
    tpa.draw_tree(ms1, ms2)
    tva.draw_tree(ms1, ms2)
    tfppa.draw_tree(ms1, ms2)