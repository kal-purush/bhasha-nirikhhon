#
# Input: CSV file with edges (pairs of nodes)
# Output: Similar type file whith only those pairs, which have at least N1 common friends
#

in_file_name="../data/typical_mpgu_pairs.csv"
out_file_name ="../data/typical_mpgu_strong_edges.csv"
N1 = 10

nodes = set()
edges = []
friends = dict()

# make an ordered pair from two elements
def make_pair(id1, id2):
    if (id1 > id2):
        return [id1, id2]
    else:
        return [id2, id1]


def edges2triangles():
    
    out_file = open(out_file_name, 'w')

    in_file = open(in_file_name, 'r', encoding = 'utf-8')
    data = in_file.readlines()
    in_file.close()

    for dataline in data:
        [id1, id2] = dataline.rstrip().split("\t")
        nodes.add(id1)
        nodes.add(id2)
        edges.append(make_pair(id1, id2))
        
    for id in nodes :
        friends[id] = []
    
    for [id1, id2] in edges:
        friends[id1].append(id2)
        friends[id2].append(id1)


    for [id1, id2] in edges:
        edge_triangles = set(friends[id1]) & set(friends[id2])
        if len(edge_triangles) > N1:
            out_file.write(id1 + "\t" + id2 + "\n")
        #print (id1, id2, edge_triangles)

    print (len(edges), len(nodes))
    out_file.close()

edges2triangles()