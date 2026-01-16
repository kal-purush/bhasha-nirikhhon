
from utils import read_csv_file
import vocabulary as mv
import dataset as md
import torch.utils.data as tud
import torch

mol_list, target_list, activity_list = zip(*read_csv_file('./data/train.csv', num_fields=3))


vocabulary = mv.create_vocabulary(smiles_list=mol_list, tokenizer=mv.SMILESTokenizer())
Dataset = md.Dataset(mol_list,target_list, activity_list, vocabulary, mv.SMILESTokenizer())
coldata = tud.DataLoader(Dataset, 2, collate_fn=Dataset.collate_fn,shuffle=True)

for encoded_seq, target, activity in coldata:
    print(encoded_seq.size())
    print(len(target))
    print(len(activity))
    print(target, activity)