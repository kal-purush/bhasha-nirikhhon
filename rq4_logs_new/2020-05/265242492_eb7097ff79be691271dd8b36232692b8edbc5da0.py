import torch
import numpy as np
import random
from torch.utils.data import Dataset

class TCNDataset(Dataset):
    def __init__(self, num_classes, actions_dict, gt_path, features_path, sample_rate, vid_list_file):
        self.list_of_examples = list()
        self.index = 0
        self.num_classes = num_classes
        self.actions_dict = actions_dict
        self.gt_path = gt_path
        self.features_path = features_path
        self.sample_rate = sample_rate
        file_ptr = open(vid_list_file, 'r')
        self.list_of_examples = file_ptr.read().split('\n')[:-1]
        file_ptr.close()
        random.shuffle(self.list_of_examples)

    def __len__(self):
        return len(self.list_of_examples)

    def __getitem__(self, idx):
        sample = self.list_of_examples[idx]

        features = np.load(self.features_path + sample.split('.')[0] + '.npy')
        file_ptr = open(self.gt_path + sample, 'r')
        content = file_ptr.read().split('\n')[:-1]
        classes = np.zeros(min(np.shape(features)[1], len(content)))
        for i in range(len(classes)):
            classes[i] = self.actions_dict[content[i]]
        batch_input = features[:, ::self.sample_rate]
        batch_target = classes[::self.sample_rate]

        length_of_sequences = list(map(len, batch_target))
        batch_input_tensor = torch.zeros(np.shape(batch_input[0])[0], max(length_of_sequences), dtype=torch.float)
        batch_target_tensor = torch.ones(max(length_of_sequences), dtype=torch.long)*(-100)
        mask = torch.zeros(self.num_classes, max(length_of_sequences), dtype=torch.float)
        
        batch_input_tensor[:, :np.shape(batch_input)[1]] = torch.from_numpy(batch_input)
        batch_target_tensor[:np.shape(batch_target)[0]] = torch.from_numpy(batch_target)
        mask[:, :np.shape(batch_target[i])[0]] = torch.ones(self.num_classes, np.shape(batch_target)[0])

        return {
                "input": batch_input_tensor,
                "target": torch.Tensor(
                    batch_target_tensor).type(torch.LongTensor),
                "mask": torch.Tensor(
                    mask),
            }