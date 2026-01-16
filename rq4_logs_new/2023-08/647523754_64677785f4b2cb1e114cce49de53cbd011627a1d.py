import torch
import os
import pickle
from collections import defaultdict
from transformers import AutoTokenizer
from utils.misc import invert_dict
import re
def collate(batch):
    batch = list(zip(*batch))
    topic_entity, question, answer, entity_range,relation_embedd = batch
    topic_entity = torch.stack(topic_entity)
    question = {k:torch.cat([q[k] for q in question], dim=0) for k in question[0]}
    answer = torch.stack(answer)
    entity_range = torch.stack(entity_range)
    # relation_embedd = {k:torch.cat([q[k] for q in relation_embedd[0]], dim=0) for k in relation_embedd[0][0]}
    # relation_embedd = {k: torch.cat([q[k] for q in relation_embedd], dim=0) for k in relation_embedd[0]}

    # relation_embedd = {k: torch.cat([q[k] for q in relation_embedd], dim=0) for k in relation_embedd[0]}
    relation_embedd = relation_embedd[0]
    return topic_entity, question, answer, entity_range,relation_embedd


class Dataset(torch.utils.data.Dataset):
    def __init__(self, questions, ent2id,relation_embedd):
        self.questions = questions
        self.ent2id = ent2id
        self.relation_embedd=relation_embedd

    def __getitem__(self, index):
        topic_entity, question, answer, entity_range = self.questions[index]
        topic_entity = self.toOneHot(topic_entity)
        answer = self.toOneHot(answer)
        entity_range = self.toOneHot(entity_range)
        relation_embedd = self.relation_embedd
        return topic_entity, question, answer, entity_range,relation_embedd

    def __len__(self):
        return len(self.questions)

    def toOneHot(self, indices):
        indices = torch.LongTensor(indices)
        vec_len = len(self.ent2id)
        one_hot = torch.FloatTensor(vec_len)
        one_hot.zero_()
        one_hot.scatter_(0, indices, 1)
        return one_hot


class DataLoader(torch.utils.data.DataLoader):
    def __init__(self, input_dir, fn, bert_name, ent2id, rel2id, batch_size, relation_embedd,training=False):
        print('Reading questions from {}'.format(fn))
        self.tokenizer = AutoTokenizer.from_pretrained(bert_name)
        self.ent2id = ent2id
        self.rel2id = rel2id
        self.id2ent = invert_dict(ent2id)
        self.id2rel = invert_dict(rel2id)



        sub_map = defaultdict(list)
        so_map = defaultdict(list)
        for line in open(os.path.join(input_dir, 'kb2_r.txt')):
            l = line.strip().split('\t')
            s = l[0]
            p = l[1].strip()
            o = l[2]
            sub_map[s].append((p, o))
            so_map[(s, o)].append(p)


        data = []
        for line in open(fn):
            line = line.strip()
            if line == '':
                continue
            line = line.split('\t')
            # if no answer
            # if len(line) != 2:
            #     continue
            question = line[0]
            question = re.sub('\.|_', ' ', question)
            # question_1 = question[0]
            # question_2 = question[1].split(']')
            head = line[1]
            # question_2 = question_2[1]
            # question = question_1 + 'NE' + question_2
            # question = question_1.strip()
            ans = line[2]

            # if (head, ans[0]) not in so_map:
            #     continue

            entity_range = set()
            list1=[]
            list2=[]
            # list3=[]
            for p, o in sub_map[head]:
                entity_range.add(o)
                list1.append(p)
                for p2, o2 in sub_map[o]:
                    entity_range.add(o2)
                    list2.append(p2)
                    # for p3, o3 in sub_map[o2]:
                    #     entity_range.add(o3)
                    #     list3.append(p3)
            entity_range = [ent2id[o] for o in entity_range]
            # relation_range = (list1,list2,list3)
            head = [ent2id[head]]
            question = self.tokenizer(question.strip(), max_length=64, padding='max_length', return_tensors="pt")
            ans = [ent2id[ans]]
            data.append([head, question, ans, entity_range])

        print('data number: {}'.format(len(data)))
        # relation_embedd={k: torch.cat([q[k] for q in relation_embedd], dim=0) for k in relation_embedd[0]}
        dataset = Dataset(data, ent2id,relation_embedd)

        super().__init__(
            dataset, 
            batch_size=batch_size,
            shuffle=training,
            collate_fn=collate, 
            )


def load_data(input_dir, bert_name, batch_size):
    cache_fn = os.path.join(input_dir, 'processed.pt')
    if os.path.exists(cache_fn):
        print('Read from cache file: {} (NOTE: delete it if you modified data loading process)'.format(cache_fn))
        with open(cache_fn, 'rb') as fp:
            # ent2id, rel2id, triples, train_data,val_data,test_data = pickle.load(fp)
            ent2id, rel2id, triples, train_data, test_data = pickle.load(fp)
        print('Train number: {}, test number: {}'.format(len(train_data.dataset), len(test_data.dataset)))
    else:
        print('Read data...')
        ent2id = {}
        for line in open(os.path.join(input_dir, 'entity_r2.txt')):
            # l = line.replace('<', '').replace('>', '').replace('"', '')
            l=line.strip()
            ent2id[l] = len(ent2id)
        # print(len(ent2id))
        # print(max(ent2id.values()))
        rel2id = {}
        for line in open(os.path.join(input_dir, 'relation2_r.txt')):
            # l = line.replace('.', '').replace('<', '').replace('>', '').replace('"', '')
            l = line.strip()
            rel2id[l] = len(rel2id)
            # rel2id[l + '_reverse'] = len(rel2id)
            # f = open("2_relation.txt",'a+')
            # f.write(l)
            # f.write('\n')
            # f.write(l + '_reverse')
            # f.write('\n')


        triples = []
        for line in open(os.path.join(input_dir, 'kb2_r.txt')):
            l = line.strip().split('\t')
            s = ent2id[l[0]]
            p = rel2id[l[1].strip()]
            o = ent2id[l[2]]
            triples.append((s, p, o))
            # p_rev = rel2id[l[1].strip()+'_reverse']
            # triples.append((o, p_rev, s))
        triples = torch.LongTensor(triples)
        tokenizer = AutoTokenizer.from_pretrained(bert_name)
        relation_embedd=[]
        for line in open(os.path.join(input_dir, 'relation2_r.txt')):
            l = line.strip()
            relation = re.sub('\.|_', ' ', l)
            # sourse_level=l[0].strip()
            # relation_level = sourse_level.split('.')[-1]
            # relation = l
            # token = tokenizer.tokenize(relation)
            relation_e=tokenizer(relation, max_length=20, padding='max_length', return_tensors="pt")
            relation_embedd.append(relation_e)
            # relation.append(sourse_level)
            # relation.append(relation_level)
            # relation.append(type_level)
        relation_embedd=tuple(relation_embedd)
        # i=0
        # for k in relation_embedd[0]:
        #     # for q in relation_embedd:
        #     l=torch.cat([q[k] for q in relation_embedd], dim=0)
        #     relation_embedd={k:l}
        relation_embedd = {k: torch.cat([q[k] for q in relation_embedd], dim=0) for k in relation_embedd[0]}
        train_data = DataLoader(input_dir, os.path.join(input_dir, 'QA/train_asr.txt'), bert_name, ent2id, rel2id, batch_size,relation_embedd, training=True)
        # val_data = DataLoader(input_dir, os.path.join(input_dir, 'QA/2q_val.txt'), bert_name, ent2id, rel2id, batch_size, relation_embedd)
        test_data = DataLoader(input_dir, os.path.join(input_dir, 'QA/test_asr3.txt'), bert_name, ent2id, rel2id, batch_size,relation_embedd)
    
        with open(cache_fn, 'wb') as fp:
            # pickle.dump((ent2id, rel2id, triples, train_data,val_data,test_data), fp)
            pickle.dump((ent2id, rel2id, triples, train_data, test_data), fp)

    # return ent2id, rel2id, triples, train_data,val_data,test_data
    return ent2id, rel2id, triples, train_data,test_data