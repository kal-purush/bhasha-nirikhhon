import networkx as nx
import json
from networkx.readwrite import json_graph
from tqdm import tqdm


# def build_citation_network(citations):
#     G = nx.DiGraph()
#     for citation in tqdm(citations):
#         citing_paper = citation["context_id"].split('_')[0]
#         cited_paper = citation["positive_ids"][0]
#         G.add_edge(citing_paper, cited_paper)
#     return G
# with open('arxiv/train.json', 'r', encoding='utf-8') as f:
#     train_data = json.load(f)
#     print(len(train_data))
# with open('arxiv/val.json', 'r', encoding='utf-8') as f:
#     val_data = json.load(f)
#     print(len(val_data))
# with open('arxiv/test.json', 'r', encoding='utf-8') as f:
#     test_data = json.load(f)
#     print(len(test_data))
# data = train_data + val_data + test_data    


def build_citation_network(data):
    G = nx.DiGraph()
    for key, value in tqdm(data.items()):
        citing_id = value["citing_id"]
        refid = value["refid"]
        G.add_edge(citing_id, refid)
    return G
    
with open('refseer/contexts.json', 'r', encoding='utf-8') as f:
    context_data = json.load(f)
    print(len(context_data))    


citation_network = build_citation_network(context_data)
network_data = json_graph.node_link_data(citation_network)
print(citation_network.number_of_nodes())

with open('refseer/citation_network.json', 'w') as outfile:
    json.dump(network_data, outfile)
