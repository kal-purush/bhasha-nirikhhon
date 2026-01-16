import pandas as pd
from datasets import load_dataset
from transformers import BertTokenizer, AutoTokenizer
import torch
import numpy as np 
from utils import * 
from torch.utils.data import Dataset, DataLoader
from models import *


TRAIN_PATH = './data/UIT-ViSFD/Train.csv'
VAL_PATH = './data/UIT-ViSFD/Dev.csv'
TEST_PATH = './data/UIT-ViSFD/Test.csv'


df_train = pd.read_csv(TRAIN_PATH,  encoding = 'utf8')
df_val = pd.read_csv(VAL_PATH, encoding = 'utf8')
df_test = pd.read_csv(TEST_PATH, encoding = 'utf8')


tokenizer = AutoTokenizer.from_pretrained('vinai/phobert-base')




categories = get_categories(df_train)
dataset = process_data(df_train, categories)
data_collator = SentimentDataCollator(tokenizer)
dataloader = DataLoader(dataset, batch_size=64, collate_fn=data_collator)

      
model = ABSA_Tree_transfomer( vocab_size= tokenizer.vocab_size, N= 12, d_model= 768, d_ff= 2048, h= 12, dropout = 0.1, num_categories = len(categories) , no_cuda=False)



device = 'cuda'

model.to(device)

for batch in tqdm(dataloader):
    inputs = batch['input_ids'].to(device)
    mask = batch['attention_mask'].to(device)
    labels = batch['labels'].to(device)

    output = model(inputs, mask, categories)
    print(output.shape)
    break

    
# class PositionalEncoding(nn.Module):
#     def __init__(self, d_model, max_len=512):
#         super(PositionalEncoding, self).__init__()
#         self.dropout = nn.Dropout(p=0.1)
        
#         # Compute the positional encodings once in log space
#         pe = torch.zeros(max_len, d_model)
#         position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
#         div_term = torch.exp(torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model))
#         pe[:, 0::2] = torch.sin(position * div_term)
#         pe[:, 1::2] = torch.cos(position * div_term)
#         pe = pe.unsqueeze(0)  # Add batch dimension
#         self.register_buffer('pe', pe)
        
#     def forward(self, x):
#         # Ensure that positional encoding matches the shape of the input tensor x
#         pe = self.pe[:, :x.size(1)]  # Slice positional encoding along the sequence length dimension
#         pe = pe.expand(x.size(0), -1, -1)  # Expand positional encoding to match batch size
#         pe = pe.to(x.device)  # Move positional encoding to the same device as input tensor
#         x = x + pe
#         return self.dropout(x)

# # Example usage:
# d_model = 768  # Dimensionality of the model
# max_len = 128 # Maximum sequence length
# positional_encoding = PositionalEncoding(d_model, max_len)

# # Assuming x is your input tensor of shape (batch_size, sequence_length, d_model)
# x = torch.randn(32, 128, d_model).cuda() # Example input tensor
# output = positional_encoding(x)
# print(output)