import snap
import sys

def extract_domain(line):
	domain_raw = line.split('\t')[1]
	temp = domain_raw.split('/')
	domain_net = temp[0] + temp[1] + temp[2]
	domain_net = domain_net.replace('\n', '')
	return domain_net

filename = '../' + sys.argv[1]
domain_mapping = []
counter = int(sys.argv[2])
last_domain = -1
testing_line = ''

g1 = snap.TNGraph.New()
with open(filename) as infile:
	for line in infile:
		if('P' in line):
			testing_line += line
			counter -= 1
			domain_name = extract_domain(line)
			nodeid = -1
			if domain_name in domain_mapping:
				nodeid = domain_mapping.index(domain_name)
			else:
				domain_mapping.append(domain_name)
				nodeid = len(domain_mapping) - 1
				g1.AddNode(nodeid)
			last_domain = nodeid

		elif('L' in line):
			testing_line += line
			domain_name = extract_domain(line)
			node_link_id = -1
			if domain_name in domain_mapping:
				node_link_id = domain_mapping.index(domain_name)
			else:
				domain_mapping.append(domain_name)
				node_link_id = len(domain_mapping) - 1
				g1.AddNode(node_link_id)

			print "add edge from %d to %d" % (last_domain, node_link_id)
			g1.AddEdge(last_domain, node_link_id)

		if (counter == 0):
			break

print "\n======================================"
print "Node Mapping : "
print "======================================\n"
for i in range(len(domain_mapping)):
	print "%s ----> %d " % (domain_mapping[i], i)

print "\n======================================"
print "Node Iterate : "
print "======================================\n"
for NI in g1.Nodes():
	print "domain %s, outdegree = %d, indegree = %d" % (domain_mapping[NI.GetId()], NI.GetOutDeg(), NI.GetInDeg())