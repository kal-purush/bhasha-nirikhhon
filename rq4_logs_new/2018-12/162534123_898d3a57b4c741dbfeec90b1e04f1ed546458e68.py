import torch.nn as nn
import torch.nn.functional as F
'''
    @model: BiLSTMwithAttn
    @params:
        V: (int)Vocab_size
        D: (int) embedding_dim
        hidden_dim: (int) hidden_dim
        num_layers: (int) lstm stack的层数
        attn_method: (str)注意力机制的方法
                     两种：
                     ‘simple': 直接将各个hidden转换为一个scalar
                     ‘complex': 先将hidden映射到另一个向量空间，然后计算它与一个单词上下文向量u的相似度,最后得到这个scalar
        attn_dim: (int) 注意力空间的dim
'''
class Attn(nn.Module):
    def __init__(self,hidden_dim,attn_dim,attn_method):
        super(Attn,self).__init__()
        self.attn_method = attn_method
        if attn_method=='simple':
            self.projection = nn.Linear(hidden_dim,1)
        elif attn_method=='complex':
            self.projection = nn.Linear(hidden_dim,attn_dim)
            self.u = nn.Parameter(torch.randn(attn_dim,1))
    # 输入的shape: (batch,len,hidden_dim)
    def forward(self,hiddens):
        energy = F.tanh(self.projection(hiddens))
        if self.attn_method=='complex':
            energy = energy.matmul(self.u)
        attn_weights = F.softmax(energy.squeeze(2),dim=1).unsqueeze(1)
        return attn_weights # shape: (batch,1,len)
class BiLSTMwithAttn(nn.Module):
    def __init__(self,V,D,hidden_dim=150,num_layers=2,attn_method='basic',attn_dim=196):
        super(BiLSTMwithAttn,self).__init__()
        self.embedding = nn.Embedding(V,D)
        self.emb_dropout = nn.Dropout(p=0.3,inplace=True)
        self.encoder = nn.LSTM(D,
                               hidden_dim, 
                               num_layers=num_layers,
                               bidirectional=True,
                               dropout=0.5)
        self.attn = Attn(hidden_dim*2,attn_dim,attn_method)
        self.predictor = nn.Linear(hidden_dim*2,3)
    def forward(self, seq):
        seq = self.embedding(seq)
        self.emb_dropout(seq)
        hiddens, _ = self.encoder(seq)
        attn_weigths = self.attn(hiddens.transpose(0,1))    # shape: (batch,1,len)
        contexts = attn_weigths.bmm(hiddens.transpose(0,1)) # shape: (batch,1,hidden_dim)
        preds = self.predictor(contexts.squeeze(1))
        return F.log_softmax(preds)