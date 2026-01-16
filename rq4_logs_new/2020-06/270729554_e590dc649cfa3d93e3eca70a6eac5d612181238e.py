import csrgraph as cg
import nodevectors
import pickle
import numpy as np
"""
使用 csr 矩阵加速计算 node2vec
"""

if __name__ == "__main__":
    # read_path = "../data/Node2Vec/Input/karate.edgelist"
    read_path = "../data/Node2Vec/Input/edges_2017_all_undirected.csv"
    save_path = "../data/Node2Vec/Output/edges_2017_all_undirected.emb"
    # save_path = "../data/Node2Vec/Output/karate.npy"
    print("1")
    G = cg.read_edgelist(read_path, sep=' ', index_col=False)
    print("2")
    ggvec_model = nodevectors.GGVec()
    print("3")
    embeddings = ggvec_model.fit_transform(G)
    print("4")
    with open(save_path, 'wb') as f:
        np.save(f, embeddings)
    print("5")