import re
import networkx as nx
import gensim
from nodevectors import Node2Vec
import matplotlib.pyplot as plt
import csrgraph as cg
from nodevectors.embedders import BaseNodeEmbedder
from sklearn.model_selection import train_test_split
import subprocess
import numba
import numpy as np
import pandas as pd
import time
import warnings
import torch
from torch import nn
from torch_geometric.data import Data
from torch_geometric.utils import to_networkx
import torch_geometric.transforms as T
from torch_geometric.nn import GCNConv
import torch.nn.functional as F
from sklearn.metrics import roc_auc_score
from torch_geometric.utils import negative_sampling
from torch_geometric.transforms import RandomLinkSplit
import requests
import torch_scatter

# Gensim triggers automatic useless warnings for windows users...
warnings.simplefilter("ignore", category=UserWarning)

warnings.simplefilter("default", category=UserWarning)


class Node2Vec2(BaseNodeEmbedder):
    def __init__(
        self,
        n_components=32,
        walklen=30,
        epochs=20,
        return_weight=1.,
        neighbor_weight=1.,
        threads=0,
        keep_walks=False,
        verbose=True,
        w2vparams={"window":10, "negative":5, "iter":10,
                   "batch_words":128}):
      
       
      
        if type(threads) is not int:
            raise ValueError("Threads argument must be an int!")
        if walklen < 1 or epochs < 1:
            raise ValueError("Walklen and epochs arguments must be > 1")
        self.n_components = n_components
        self.walklen = walklen
        self.epochs = epochs
        self.keep_walks = keep_walks
        if 'size' in w2vparams.keys():
            raise AttributeError("Embedding dimensions should not be set "
                + "through w2v parameters, but through n_components")
        self.w2vparams = w2vparams
        self.return_weight = return_weight
        self.neighbor_weight = neighbor_weight
        if threads == 0:
            threads = numba.config.NUMBA_DEFAULT_NUM_THREADS
        self.threads = threads
        w2vparams['workers'] = threads
        self.verbose = verbose

    def fit(self, G):
     
       
       
        if not isinstance(G, cg.csrgraph):
            G = cg.csrgraph(G, threads=self.threads)
        if G.threads != self.threads:
            G.set_threads(self.threads)
        # Because networkx graphs are actually iterables of their nodes
        #   we do list(G) to avoid networkx 1.X vs 2.X errors
        node_names = G.names
        if type(node_names[0]) not in [int, str, np.int32, np.uint32,
                                       np.int64, np.uint64]:
            raise ValueError("Graph node names must be int or str!")
        # Adjacency matrix
        walks_t = time.time()
        if self.verbose:
            print("Making walks...", end=" ")
        self.walks = G.random_walks(walklen=self.walklen,
                                    epochs=self.epochs,
                                    return_weight=self.return_weight,
                                    neighbor_weight=self.neighbor_weight)
        if self.verbose:
            print(f"Done, T={time.time() - walks_t:.2f}")
            print("Mapping Walk Names...", end=" ")
        map_t = time.time()
        self.walks = pd.DataFrame(self.walks)
        # Map nodeId -> node name
        node_dict = dict(zip(np.arange(len(node_names)), node_names))
        for col in self.walks.columns:
            self.walks[col] = self.walks[col].map(node_dict).astype(str)
        # Somehow gensim only trains on this list iterator
        # it silently mistrains on array input
        self.walks = [list(x) for x in self.walks.itertuples(False, None)]
        if self.verbose:
            print(f"Done, T={time.time() - map_t:.2f}")
            print("Training W2V...", end=" ")
            if gensim.models.word2vec.FAST_VERSION < 1:
                print("WARNING: gensim word2vec version is unoptimized"
                    "Try version 3.6 if on windows, versions 3.7 "
                    "and 3.8 have had issues")
        w2v_t = time.time()
        # Train gensim word2vec model on random walks
        self.model = gensim.models.Word2Vec(
            sentences=self.walks,
            vector_size=self.n_components, #!!!!
            **self.w2vparams)
        if not self.keep_walks:
            del self.walks
        if self.verbose:
            print(f"Done, T={time.time() - w2v_t:.2f}")

    def fit_transform(self, G):
      
        if not isinstance(G, cg.csrgraph):
            G = cg.csrgraph(G, threads=self.threads)
        self.fit(G)
        w = np.array(
            pd.DataFrame.from_records(
            pd.Series(np.arange(len(G.nodes())))
              .apply(self.predict)
              .values)
        )
        return w

    def predict(self, node_name):
      
        # current hack to work around word2vec problem
        # ints need to be str -_-
        if type(node_name) is not str:
            node_name = str(node_name)
        return self.model.wv.__getitem__(node_name)

    def save_vectors(self, out_file):

     
        self.model.wv.save_word2vec_format(out_file)

    def load_vectors(self, out_file):
       
        self.model = gensim.wv.load_word2vec_format(out_file)
     

class GraphData:
  edge_index = []
  edge_list = []
  nodes = []
  X = []
  circles = []
  X_names = []
  embeddings = []

  def __init__(self, edges, edge_list, X, circles, X_names, embeddings, nodes):
    self.edge_index = edges
    self.edge_list = edge_list
    self.X = X
    self.circles = circles
    self.X_names = X_names
    self.embeddings = embeddings
    self.nodes = nodes

def visualize(g, title="Graph", edge='blue'):
    pos = nx.kamada_kawai_layout(g)
    plt.figure(figsize=(10, 10))
    plt.title(title)
    plt.axis('on')
    nx.draw_networkx(g, pos=pos, node_size=10,
                     arrows=False, width=1, style='solid', with_labels= False)
    plt.savefig('src/graph.png')

    #This function is for split a graph data's edges into test, validation and train set
def split_data(graph, embeddings_type = 0):
  split = RandomLinkSplit(
      num_val=0.05,  # size of the validation set
      num_test=0.1,  # size of the test set
      is_undirected=True,
      add_negative_train_samples=False, # We do not want negativ edges in the traing set
      neg_sampling_ratio=1.0,   # In the test and validation set the num of negativ edges will be the same as the positives
  )
  # embedding_type is 0, when we want to use the embeddings of the nodes, given by the Node2Vec algorithm
  # if its 1, we use the original representations of the nodes (X) and when it's two we use both of them
  if embeddings_type == 0:
     data = Data(x=graph.embeddings, edge_index=graph.edge_index)
  elif embeddings_type == 1:
     data = Data(x=graph.X, edge_index=graph.edge_index) 
  else:
     data = Data(x=torch.cat((graph.X, graph.embeddings), dim=1), edge_index=graph.edge_index)
     
        
  train_data, val_data, test_data = split(data)

  print('train_data:', train_data)
  print('val_data:', val_data)
  print('test_data:', test_data)
  return train_data, val_data, test_data


class GNNVAE(nn.Module):
  def __init__(self, in_channels, hidden_channels, out_channels):
    super(GNNVAE,self).__init__()
    # GCN layers
    self.GNNConvIn = GCNConv(in_channels=in_channels, out_channels=hidden_channels)
    self.GNNConvHidden = GCNConv(in_channels=hidden_channels, out_channels=hidden_channels)
    self.GNNConvOut = GCNConv(in_channels=hidden_channels, out_channels=out_channels)

  # The encoder part is a GCN network
  def encode(self,x,edge_index):
    x = self.GNNConvIn(x,edge_index).relu()
    x = self.GNNConvOut(x,edge_index).relu()
    return x

  # The decoder part of the network 
  def decode(self,z,edges):
    return (z[edges[0]] * z[edges[1]]).sum(dim=-1)

  # training function
  def train_GNNVAE(self, train_dataset,val_dataset,optimizer,epoch,verbose=False):
    for epoch in range(0,epoch):
      self.train()
      optimizer.zero_grad()
      # Transport data to GPU if nessesery
      x, edge_index, edge_label_index = train_dataset.x.to(device), train_dataset.edge_index.to(device), train_dataset.edge_label_index.to(device)
     
      # The autoencoder's hidden representation
      z = self.encode(x,edge_index)
      
      # Adding negativ edges to the graph
      # The number of negativ edges will be the same as the positiv edges (num_neg_samples=len(edge_label_index[1]))
      neg_edge_index = negative_sampling(
              edge_index=edge_index, num_nodes=torch.tensor(train_dataset.num_nodes).to(device),
              num_neg_samples=len(edge_label_index[1]), method='sparse')
      
      # Adding the negativ edges to the original (positiv) edges of the graph 
      pos_neg_edge_index = torch.cat(
          [edge_label_index, neg_edge_index],
          dim=-1,
      )
    
      # Creating the ground truth labels for the edges: 0 if it's a negativ and 1 if it's a positiv edge
      edge_y = torch.cat([
          torch.ones(neg_edge_index.size(1)).to(device),
          train_dataset.edge_label.new_zeros(neg_edge_index.size(1)).to(device)
      ], dim=0)
      
      # decoder part of the autoencoder
      out = self.decode(z,pos_neg_edge_index)

      # Definiing our loss function
      loss_fn = torch.nn.BCEWithLogitsLoss()
      loss = loss_fn(out,edge_y)
      loss.backward()
      optimizer.step()

      # validation step
      if (verbose and epoch % 10 == 0):
        val_roc_auc = self.eval_GNNVAE(val_dataset)
        print(f"Train loss: {loss}\nValidation AUC: {val_roc_auc}")
    return self


  # Custum eval function
  @torch.no_grad()
  def eval_GNNVAE(self, data):
    self.eval()
     # Transport data to GPU if nessesery
    x, edge_index, edge_label_index = data.x.to(device), data.edge_index.to(device), data.edge_label_index.to(device)
    # The validation and test data already contain negative edges via Graph Link Split
    # So in this step we do not need to manually add them
    z = self.encode(x, edge_index)
    out = self.decode(z, edge_label_index).view(-1).sigmoid()
    #print(data.edge_label.cpu().numpy()[0:50],  np.round(out.cpu().numpy())[0:50])
    #print(data.edge_label.cpu().numpy()[-100:-50],  np.round(out.cpu().numpy())[-100:-50])
    acc = (data.edge_label.cpu().numpy() == np.round(out.cpu().numpy())).sum() / len(data.edge_label.cpu().numpy())
    print(acc)
    roc =  roc_auc_score(data.edge_label.cpu().numpy(), out.cpu().numpy())
    return roc, acc


if __name__ == '__main__':
  
  nx.adj_matrix = nx.adjacency_matrix
  # get the ego indexes
  with open('facebook/indexes.txt', 'r') as f:
    Fb_ego_indexes= [int(line.strip()) for line in f]

  Facebook_graphs = []

  for index in range(len(Fb_ego_indexes)):
  
    # First, we load the edges and save them in two different format: edges is a list of the edge pairs and
    # edge_index conitains the edges in a two dimensional matrix, each index of this matrix represnt an edge, this
    # is useful if we use pytorcg geometric for handle the graph data
    f = open(f'facebook/{Fb_ego_indexes[index]}.edges', "rt")
    edges_str = f.readlines()
    edge_index = [list(map(int, item.strip().split())) for item in edges_str]
    # Transpose the data to get desired format
    edge_index = torch.tensor([list(row) for row in zip(*edge_index)])
    edge_list =   [(int(x.split()[0]), int(x.split()[1])) for x in edges_str]
    f = open(f'facebook/{Fb_ego_indexes[index]}.circles', "rt")
    
    # These are the social circles in the graph
    circles_str = f.readlines()
    circles = [list(map(int, item.split('\t')[1:])) for item in circles_str]

    #Here we collect the name of each feauture in the graph
    f = open(f'facebook/{Fb_ego_indexes[index]}.featnames', "rt")
    feat_name_str = f.readlines()
    X_names = [item.split(';anonymized feature')[0].split(';') for item in feat_name_str]

    # These are the features for the "ego" of the graph (ego means the person whose connenctions are in the given subgraph)
    #f = open(f'facebook/{Fb_ego_indexes[index]}.egofeat', "rt")
    #feat_str = f.readlines()
    #X = []
    #X.append(list(map(int, feat_str[0].split()[1:])) )
    #X = torch.tensor(X)
    # These are the features for all the nodes of the graph, except the ego
    f = open(f'facebook/{Fb_ego_indexes[index]}.feat', "rt")
    feat_str = f.readlines()
    
    index = torch.tensor([int(list(map(int, item.split()))[0]) for item in feat_str])
    # töröljük az olyan indexű csúcsokat amik mar voltak
    X_ego = torch.tensor([list(map(int, item.split()[1:]))[0:-1] for item in feat_str])
    #X = torch.cat([ torch.empty(), X_ego], dim=0)
    X = X_ego

      
    # Node mapping
    node_mapping = {old.item(): new  for new, old in enumerate(index)}
                                        
    mapped_edge_index = torch.tensor([[node_mapping[n.item()] for n in edge_index[0]],
                                  [node_mapping[n.item()] for n in edge_index[1]]])
          
    # Here we convert our graph, because Node2Vec expects an NX graph     
    G = nx.from_edgelist(mapped_edge_index.T.numpy())
    # When we transform the graph this way, we lose the isolated nodes ( the ones that only connect with the ego)
    # so with an extra step we add extra nodes to represent the isolated nodes
    max_ = mapped_edge_index[0, :].max()
    for node_id in range(max_ -1):
      if node_id not in G:
          G.add_node(node_id)
        
    # creating the Node2Vec embeddings 
    n2v = Node2Vec2(n_components=64, walklen=10, epochs=50, return_weight=1.0, neighbor_weight=1.0, threads=0, w2vparams={'window': 4, 'negative': 5, 'epochs':10, 'ns_exponent': 0.5, 'batch_words': 128})

    # Fit and get the embedding
    n2v.fit(G)
    #Getting the nodes of the graph
    nodes = G.nodes()
    embeddings = []
    for node in nodes:
      embeddings.append(n2v.predict(node))
    # Create the subgraph
    Facebook_graphs.append(GraphData(mapped_edge_index, edge_list, X.float(), circles, X_names, torch.tensor(embeddings), nodes))



  
  Edge_graph = nx.from_edgelist(Facebook_graphs[0].edge_list)
  visualize( Edge_graph, "First graph")

  device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
  #TRAINING
  print("______ I. Method: Using the Node2Vec embeddings  ___________")
  n2v_test_aucs = []
  n2v_test_acc = []
  for i in range(len(Facebook_graphs)):
    print(i+1, ". graph:")
    graph = Facebook_graphs[i]
    train_data, val_data, test_data = split_data(graph, 0)
    model = GNNVAE(train_data.x.shape[1], 32, 16).to(device)
    optimizer = torch.optim.Adam(params=model.parameters(), lr=0.01)

    model = model.train_GNNVAE(train_data, val_data, optimizer,300)
    test_auc, test_acc = model.eval_GNNVAE(test_data)
    n2v_test_aucs.append(test_auc)
    n2v_test_acc.append(test_acc)

    print(f"Test auc: {test_auc:.3f}, test accuracy: {test_acc:.3f}")
  print(f"Average Test AUC: {np.array(n2v_test_aucs).sum() / len(n2v_test_aucs)}")
  print(f"Average Test accuracy: {np.array(n2v_test_acc).sum() / len(n2v_test_acc)}\n\n")
    
  print("______ II. Method: Using the node features  ___________")
  f_test_aucs = []
  f_test_acc = []
  for i in range(len(Facebook_graphs)):
    graph = Facebook_graphs[i]
    train_data, val_data, test_data = split_data(graph, 2)
    model = GNNVAE(train_data.x.shape[1], 32, 16).to(device)
    optimizer = torch.optim.Adam(params=model.parameters(), lr=0.01)

    model = model.train_GNNVAE(train_data, val_data, optimizer,300)
    test_auc, test_acc = model.eval_GNNVAE(test_data)
    f_test_aucs.append(test_auc)
    f_test_acc.append(test_acc)

    print(f"Test auc: {test_auc:.3f}, test accuracy: {test_acc:.3f}")
  print(f"Average Test AUC: {np.array(f_test_aucs).sum() / len(f_test_aucs)}")
  print(f"Average Test accuracy: {np.array(f_test_acc).sum() / len(f_test_acc)}\n\n")


  print("___ III. Method: Using the node features and the embeddings  ________")
  f_n2v_test_aucs = []
  f_n2v_test_acc = []
  for i in range(len(Facebook_graphs)):
    graph = Facebook_graphs[i]
    train_data, val_data, test_data = split_data(graph, 2)
    model = GNNVAE(train_data.x.shape[1], 32, 16).to(device)
    optimizer = torch.optim.Adam(params=model.parameters(), lr=0.01)

    model = model.train_GNNVAE(train_data, val_data, optimizer,300)
    test_auc, test_acc = model.eval_GNNVAE(test_data)
    f_n2v_test_aucs.append(test_auc)
    f_n2v_test_acc.append(test_acc)

    print(f"Test auc: {test_auc:.3f}, test accuracy: {test_acc:.3f}")
  print(f"Average Test AUC: {np.array(f_n2v_test_aucs).sum() / len(f_n2v_test_aucs)}")
  print(f"Average Test accuracy: {np.array(f_n2v_test_acc).sum() / len(f_n2v_test_acc)}")


  # Oszlopok pozíciói
  bar_width = 0.25
  index = np.arange(10)
  plt.figure(figsize=(20,10))
  # Oszlopdiagramok rajzolása
  plt.bar(index, n2v_test_aucs, bar_width, label='With Node2Vec')
  plt.bar(index + bar_width, f_test_aucs, bar_width, label='With original node features')
  plt.bar(index + 2*bar_width, f_n2v_test_aucs, bar_width, label='With node to vec and original features')

  # Címek és címkék

  plt.xlabel('The graphs')
  plt.ylabel('AUC scores')
  plt.title('Diagram of the AUC scores of the 3 method on each subgraf')
  plt.xticks(index + bar_width, range(10))
  plt.legend()
  plt.ylim(0.7)

  # Diagram megjelenítése
  plt.tight_layout()
  plt.savefig('src/AUC_scores.png')


   # Oszlopok pozíciói
  bar_width = 0.25
  index = np.arange(10)
  plt.figure(figsize=(20,10))
  # Oszlopdiagramok rajzolása
  plt.bar(index, n2v_test_acc, bar_width, label='With Node2Vec')
  plt.bar(index + bar_width, f_test_acc, bar_width, label='With original node features')
  plt.bar(index + 2*bar_width, f_n2v_test_acc, bar_width, label='With node to vec and original features')

  # Címek és címkék

  plt.xlabel('The graphs')
  plt.ylabel('Accuracy')
  plt.title('Diagram of the accuracy of the 3 method on each subgraf')
  plt.xticks(index + bar_width, range(10))
  plt.legend()
  plt.ylim(0.7)

  # Diagram megjelenítése
  plt.tight_layout()
  plt.savefig('src/ACC_scores.png')