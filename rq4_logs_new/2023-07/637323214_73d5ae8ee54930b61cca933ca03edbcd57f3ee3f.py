import os
import torch

# we use AutoModelForSequenceClassification instead of AutoModel as
# the former has a classification head on top of the pretrained model, and
# is easier to train. We just need to specify the number of classes.
from datasets import load_dataset
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from transformers import Trainer, TrainingArguments

# system call
os.system("")

# importing local module
from hugging_face import config
from hugging_face import utils

# device
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f'the available device is: {device}')

num_labels = 6
model = (AutoModelForSequenceClassification.from_pretrained(config.model_ckpt, num_labels=num_labels).to(device))

from huggingface_hub import interpreter_login

interpreter_login(new_session=False, write_permission=True)

emotions = load_dataset('emotion')
emotions.set_format('torch', columns=['text', 'label'])
print(emotions)

emotions_encoded = emotions.map(utils.tokenize, batched=True, batch_size=None)
print(emotions_encoded)

batch_size = 64
logging_steps = len(emotions_encoded['train']) // 64
model_name = f'{config.model_ckpt}-finetuned-emotion'
training_args = TrainingArguments(output_dir=model_name,
                                  num_train_epochs=2,
                                  learning_rate=2e-5,
                                  per_device_train_batch_size=batch_size,
                                  per_device_eval_batch_size=batch_size,
                                  weight_decay=0.01,
                                  evaluation_strategy='epoch',
                                  disable_tqdm=False,
                                  logging_steps=logging_steps,
                                  push_to_hub=False,
                                  log_level='error')

trainer = Trainer(model=model, args=training_args,
                  compute_metrics=utils.compute_metrics,
                  train_dataset=emotions_encoded['train'],
                  eval_dataset=emotions_encoded['validation'],
                  tokenizer=utils.tokenizer
                  )
trainer.train()