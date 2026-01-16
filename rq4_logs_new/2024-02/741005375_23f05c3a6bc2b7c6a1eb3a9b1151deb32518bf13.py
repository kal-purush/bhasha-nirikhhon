import torch
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv
from torch import nn

# Step 1: Define the Toy Graph
# Define edges - for a simple graph, let's say we have edges between nodes 0-1, 1-2, and 2-0 (creating a triangle)
edge_index = torch.tensor([[0, 1, 2, 1], [1, 2, 0, 0]], dtype=torch.long)  # Directed edges

# Node features - for simplicity, let's use one-hot encoded features
x = torch.eye(3)  # 3 nodes with 3 features each

# Create a graph data object
data1 = Data(x=x, edge_index=edge_index)
data2 = Data(x=x*1.1, edge_index=edge_index)
data3 = Data(x=x*5.1, edge_index=edge_index)

# Step 2: Build the GCN Model
class SimGCN(torch.nn.Module):
    def __init__(self, input_dim=4, output_dim=4):
        super(SimGCN, self).__init__()
        self.input_dict = nn.ModuleDict({
                str(int(input_dim)) : GCNConv(input_dim, output_dim)
        })

        self.conv2 = GCNConv(output_dim, output_dim)
        self.conv3 = GCNConv(output_dim, output_dim)
        self.conv4 = GCNConv(output_dim, output_dim)
        self.out_dim = output_dim

    def forward(self, x, edge_index):
        input_dim = str(int(x.shape[1]))
        if input_dim not in self.input_dict:
            self.input_dict[input_dim] = GCNConv(input_dim, self.out_dim)

        conv1 = self.input_dict[input_dim]
        x1 = conv1(x, edge_index)

        x2 = self.conv2(x1, edge_index)
        x3 = self.conv3(x2, edge_index)
        x4 = self.conv4(x3, edge_index)
        x_all = torch.cat((x1, x2, x3, x4), dim=1)
        x_all = torch.mean(x_all, dim=0)
        return x_all
    

def simple_graph_distance(data1, data2):
    global graph_similarity_model
    input_dim = data1.x.shape[1]
    if "graph_similarity_model" not in globals() or graph_similarity_model is None:
        graph_similarity_model = SimGCN(input_dim=input_dim, output_dim=3)
    
    # Get the graph embeddings by applying the model to the input data
    graph_emb1 = graph_similarity_model(data1.x, data1.edge_index)
    graph_emb2 = graph_similarity_model(data2.x, data2.edge_index)
    
    # Compute L2 distance (Euclidean distance) between graph embeddings
    l2_distance = torch.norm(graph_emb1 - graph_emb2, p=2)
    
    return l2_distance



# Initialize the GCN model with random weights
# Input dimension is 3 (as we have 3 features per node), and let's say we want 2 output features
model = SimGCN(input_dim=3, output_dim=3)

# Step 3: Apply the Model to the Graph
# Apply the model to get the embeddings
node_embeddings = model(data1.x, data1.edge_index)
print("Node embeddings:\n", node_embeddings)

node_embeddings = model(data2.x, data2.edge_index)
print("Node embeddings:\n", node_embeddings)

node_embeddings = model(data3.x, data3.edge_index)
print("Node embeddings:\n", node_embeddings)


print("Distance 1 and 2", simple_graph_distance(data1, data2))
print("Distance 1 and 3", simple_graph_distance(data1, data3))
print("Distance 2 and 3", simple_graph_distance(data2, data3))


from graphdiffusion.distance import GraphDistance
dist = GraphDistance()

print("Distance 1 and 2", dist(data1, data2))
print("Distance 1 and 3", dist(data1, data3))
print("Distance 2 and 3", dist(data2, data3))