# 1. prepare dataset
# 2. load pretrained tokenizer, call it with dataset => Encoding
# 3. build pytorch dataset with encodings
# 4. load pretrained model
# 5. - load trainer and train int
#    - native pytorch training loop


from transformers import Trainer, TrainingArguments

training_args = TrainingArguments("test-trainer")

trainer = trainer(
    model,
    training_args,
    train_dataset = tokenized_datasets["train"],
    eval_dataset = tokenized_datasets["validation"],
    data_collator = data_collator,
    tokenizer = tokenizer,
)

trainer.train()