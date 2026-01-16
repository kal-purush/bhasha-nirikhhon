import torch
import wandb
import time
import os
from tqdm import tqdm
import numpy as np
import pandas as pd
tqdm.pandas()

from datasets import load_dataset
from torch.utils.data import Dataset
from torch.utils.data import DataLoader

from transformers import AutoTokenizer, pipeline, AutoModelForSequenceClassification

from trl import AutoModelForCausalLMWithValueHead
from trl.ppo import PPOTrainer
from trl.core import build_bert_batch_from_txt, listify_batch

config = {
    "model_name": "/home/ubuntu/cloudfs/saved_models/__home__ubuntu__cloudfs__saved_models____home__ubuntu__cloudfs__huggingfacecache__bloom-1b7_1670552399_e1_step151891_loss2.337894916534424_1671125974_e1_step43135_loss0.7372605204582214",
    "cls_model_name": "lvwerra/distilbert-imdb",
    "steps": 20000,
    "batch_size": 256,
    "forward_batch_size": 16,
    "ppo_epochs": 4,   
    "txt_in_min_len": 2,
    "txt_in_max_len": 8,
    "txt_out_min_len": 4,
    "txt_out_max_len": 16,
    "lr": 1.41e-5,
    "init_kl_coef":0.2,
    "target": 6,
    "horizon":10000,
    "gamma":1,
    "lam":0.95,
    "cliprange": .2,
    "cliprange_value":.2,
    "vf_coef":.1, 
}

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
pipe_device = 0 if torch.cuda.is_available() else -1

wandb.init(name='run-1', project='bloomz-suspence-ppo', config=config)



### get dataset
###
raw_df = pd.read_csv('/home/ubuntu/cloudfs/ghost_data/newred_redbook_link_download//redbook_tags_content_to_title_sim_tags_training_data_1208_1670534928.csv')

gpt2_tokenizer = AutoTokenizer.from_pretrained(config['model_name'])
gpt2_tokenizer.pad_token = gpt2_tokenizer.eos_token

MAX_LEN = 800 #40 #192#1077 #938 #2709 #267#248
MAX_TARGET_LEN = 80 #330 #1000 #110 #120 #140 #192

BATCH_SIZE = 1 #2

class ConvDataset(Dataset):
    def __init__(self, df, verbose=False, training=True):
        super().__init__()
        char_max_len = 850

        self.df = df
        # make sure it won't truncate the title part.
        self.text = df.content.apply(lambda x: x[:char_max_len]).tolist()
        self.answer = df.title.tolist()
        self.text = [f"根据指定内容，撰写爆款小红书笔记标题：\n：需要起标题的内容：[{t}]\n小红书标题：[{a}]" for t, a in zip(self.text, self.answer)]
        # self.questions = df.t5_text.apply(lambda x: x[:x.index('chat history:')]).tolist()
        self.verbose = verbose

    def __len__(self):
        return len(self.text)

    def __getitem__(self, index):
        texts = self.text[index]

        return texts
        # question = self.questions[index]


def collate_fn(data, training=True):
    texts = data
    # print(texts)
    encoded = gpt2_tokenizer.batch_encode_plus(
        texts,
        padding='longest',
        max_length=MAX_LEN,
        truncation=True,
        return_attention_mask=True,
        return_tensors="pt",
    )

    input_ids = encoded['input_ids'].clone().detach()
    attention_masks = encoded['attention_mask'].clone().detach()

    target = encoded['input_ids']
    labels = target.clone().detach()
    labels[target == gpt2_tokenizer.pad_token_id] = -100

    return (input_ids, attention_masks, labels, texts)


# load imdb with datasets
#ds = load_dataset('imdb', split='train')
#ds = ds.rename_columns({'text': 'review', 'label': 'sentiment'})
#ds = ds.filter(lambda x: len(x["review"])>200, batched=False)


train_dataset = ConvDataset(raw_df)

train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE,
                          drop_last=True, shuffle=True, num_workers=0,collate_fn=collate_fn)


sent_kwargs = {
    "return_all_scores": True,
    "function_to_apply": "none",
    "batch_size": config["forward_batch_size"]
}


### get reward score...
###


REWARD_MODEL_NAME='IDEA-CCNL/Erlangshen-DeBERTa-v2-710M-Chinese'

reward_tokenizer = AutoTokenizer.from_pretrained(REWARD_MODEL_NAME,
                                          cache_dir="/home/ubuntu/cloudfs/huggingfacecache",
                                         )


REWARD_MODEL_NAME='/home/ubuntu/cloudfs/saved_models/IDEA-CCNL__Erlangshen-DeBERTa-v2-710M-Chinese_1672156931_e4_step499_loss0.0991830825805664'
reward_model = AutoModelForSequenceClassification.from_pretrained(REWARD_MODEL_NAME)





s#entiment_pipe = pipeline("sentiment-analysis","lvwerra/distilbert-imdb", device=pipe_device)

gpt2_model = AutoModelForCausalLMWithValueHead.from_pretrained(config['model_name'])
gpt2_model_ref = AutoModelForCausalLMWithValueHead.from_pretrained(config['model_name'])


wandb.watch(gpt2_model, log='all')

gpt2_model.to(device)
gpt2_model_ref.to(device)

class LengthSampler:
    def __init__(self, min_value, max_value):
        self.values = list(range(min_value, max_value))
    def __call__(self):
        return np.random.choice(self.values)

input_size = LengthSampler(config["txt_in_min_len"], config["txt_in_max_len"])
output_size = LengthSampler(config["txt_out_min_len"], config["txt_out_max_len"])

def tokenize(sample):
    sample["tokens"] = gpt2_tokenizer.encode(sample["review"])[:input_size()]
    sample["query"] = gpt2_tokenizer.decode(sample["tokens"])
    return sample

ds = ds.map(tokenize, batched=False)

gen_kwargs = {
    "min_length":-1,
    "top_k": 0.0,
    "top_p": 1.0,
    "do_sample": True,
    "pad_token_id": gpt2_tokenizer.eos_token_id
}

def collater(data):
    return dict((key, [d[key] for d in data]) for key in data[0])

dataloader = torch.utils.data.DataLoader(ds, batch_size=config['batch_size'], collate_fn=collater)

ppo_trainer = PPOTrainer(gpt2_model, gpt2_model_ref, gpt2_tokenizer, **config)

total_ppo_epochs = int(np.ceil(config["steps"]/config['batch_size']))

for epoch, batch in tqdm(zip(range(total_ppo_epochs), iter(dataloader))):
    logs, timing = dict(), dict()
    t0 = time.time()
    query_tensors = [torch.tensor(t).long().to(device) for t in batch["tokens"]]

    #### Get response from gpt2
    t = time.time()
    response_tensors = []
    for i in range(config['batch_size']):
        gen_len = output_size()
        response = gpt2_model.generate(query_tensors[i].unsqueeze(dim=0),
                                       max_new_tokens=gen_len, **gen_kwargs)
        response_tensors.append(response.squeeze()[-gen_len:])
    batch['response'] = [gpt2_tokenizer.decode(r.squeeze()) for r in response_tensors]
    timing['time/get_response'] = time.time()-t

    #### Compute sentiment score
    t = time.time()
    texts = [q + r for q,r in zip(batch['query'], batch['response'])]
    pipe_outputs = sentiment_pipe(texts, **sent_kwargs)
    rewards = torch.tensor([output[1]["score"] for output in pipe_outputs]).to(device)
    timing['time/get_sentiment_preds'] = time.time()-t

    #### Run PPO step 
    t = time.time()
    stats = ppo_trainer.step(query_tensors, response_tensors, rewards)
    timing['time/optimization'] = time.time()-t

    #### Log everything
    timing['time/epoch'] = time.time()-t0
    table_rows = [list(r) for r in zip(batch['query'], batch['response'], rewards.cpu().tolist())]
    logs.update({'game_log': wandb.Table(columns=['query', 'response', 'reward'], rows=table_rows)})
    logs.update(timing)
    logs.update(stats)
    logs['env/reward_mean'] = torch.mean(rewards).cpu().numpy()
    logs['env/reward_std'] = torch.std(rewards).cpu().numpy()
    logs['env/reward_dist'] = rewards.cpu().numpy()
    wandb.log(logs)