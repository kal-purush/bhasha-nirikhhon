import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
import pandas as pd
import numpy as np
import random
from datasets import Dataset
import transformers
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, Seq2SeqTrainer, Seq2SeqTrainingArguments, DataCollatorForSeq2Seq
from datasets import load_dataset, Dataset
import pandas as pd
import evaluate
import torch
import nltk
from transformers import Seq2SeqTrainer, Seq2SeqTrainingArguments, AutoTokenizer, AutoModelForSeq2SeqLM
import nltk
import argparse
import numpy as np
from transformers import AutoModelForSeq2SeqLM
from peft import get_peft_config, get_peft_model, LoraConfig, TaskType
from trl import AutoModelForCausalLMWithValueHead
from transformers import TrainingArguments, Trainer
from trl import SFTTrainer
import evaluate
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
import re
import datasets

def parse_args():
    parser = argparse.ArgumentParser(description="Run QLoRA model fine-tuning")
    parser.add_argument('--base_model_name', type=str, default='codellama/CodeLlama-7b-Instruct-hf', help="Base model name")
    parser.add_argument('--device_batch_size', type=int, default=2, help="Per device train batch size")
    parser.add_argument('--gradient_accumulation_steps', type=int, default=16, help="Gradient accumulation steps")
    parser.add_argument('--eval_samples', type=int, default=250, help="Number of samples in the eval set")
    args = parser.parse_args()
    return args

def main():
    args = parse_args()

    LANGUAGE = 'both'

    model_name_or_path = args.base_model_name
    device_batch = args.device_batch_size
    gradient_steps = args.gradient_accumulation_steps
    eval_samples = args.eval_samples

    print("Running...")

    tokenizer = AutoTokenizer.from_pretrained(model_name_or_path, 
                                              token='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')
    model = AutoModelForCausalLM.from_pretrained(model_name_or_path, 
                                                 device_map="auto", 
                                                 cache_dir='./models',
                                                 token='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'
                                                )

    if LANGUAGE in ['java', 'python']:
        ds = load_dataset("google/code_x_glue_ct_code_to_text", LANGUAGE, cache_dir='datasets')
    elif LANGUAGE == 'both':
        ds = load_dataset("doejn771/code_x_glue_ct_code_to_text_java_python")
        
    train_dataset = ds["train"]
    test_dataset = ds["validation"]

    def apply_chat_template(example):
        def remove_comments_from_code(code):
            code_without_comments = re.sub(r'""".*?"""', '', code, flags=re.DOTALL)

            # Return the cleaned code
            return code_without_comments.strip()

    #     code = remove_comments_from_code(example['code'])
        code = ' '.join(example['code_tokens'])
        language = example['language']
        chat = [{"role": "system", "content": "Provide summarization to given code segment"},
          {"role": "user", "content": f"Summarize {language} code:\n{code}"},
          {"role": "assistant", "content": f"SUMMARY: {' '.join(example['docstring_tokens'])} DONE"},
        ]
        
        if tokenizer.chat_template == None:
            tokenizer.chat_template = "{%- for message in messages -%}\n{% if message['role'] == 'system' %}{% if loop.first %}{{ message['content'] }}{% endif %}{% elif message['role'] == 'user' %}{{ 'Human: ' + message['content'] + '\n\n' }}{% elif message['role'] == 'assistant' %}{{ 'Assistant: ' + message['content'] + '\n\n' }}{% endif %}\n{%- endfor -%}\n{% if add_generation_prompt %}Human: {{ '' }}{% endif %}"

        example["text"] = tokenizer.apply_chat_template(
            chat, tokenize=False, add_generation_prompt=False)
        return example

    ex_0 = (apply_chat_template(train_dataset[0])['text'])
    ex_1 = (apply_chat_template(train_dataset[1])['text'])
    print(ex_0)

    column_names = list(train_dataset.features)

    processed_train_dataset = train_dataset.map(
        apply_chat_template,
        num_proc=10,
        remove_columns=column_names,
        desc="Applying chat template to train_sft",
    )

    all_lengths = []
    processed_test_dataset = test_dataset.map(
        apply_chat_template,
        num_proc=10,
        remove_columns=column_names,
        desc="Applying chat template to train_sft",
    )

    for i in processed_test_dataset['text']:
        all_lengths.append(tokenizer.tokenize(i).__len__())
        
    print('========== Sequence Length Per Percentiles ==========', np.percentile(all_lengths, [25,50,75]))


    # ----------------------------- training -------------------------------
    import evaluate
    rouge = evaluate.load("rouge")
    meteor = evaluate.load("meteor")
    bleu = evaluate.load("bleu")

    def compute_metrics(eval_pred):
        predictions, labels = eval_pred
        predictions = np.argmax(predictions, axis=-1)
        predictions = np.where(predictions != -100, predictions, tokenizer.eos_token_id)
        decoded_preds = tokenizer.batch_decode(predictions)
        labels = np.where(labels != -100, labels, tokenizer.eos_token_id)
        decoded_labels = tokenizer.batch_decode(labels)
        decoded_preds = [i.split("SUMMARY: ")[-1].split('DONE')[0].strip() for i in decoded_preds]
        decoded_labels = [i.split("SUMMARY: ")[-1].split('DONE')[0].strip() for i in decoded_labels]
        result = rouge.compute(predictions=decoded_preds, references=decoded_labels, use_stemmer=True)
        meteor_result = meteor.compute(predictions=decoded_preds, references=decoded_labels)
        bleu_result = bleu.compute(predictions=decoded_preds, references=decoded_labels)
        result['meteor'] = meteor_result['meteor']
        result['bleu'] = bleu_result['bleu']
        prediction_lens = [np.count_nonzero(pred != tokenizer.eos_token_id) for pred in predictions]
        result["gen_len"] = np.mean(prediction_lens)
        return {k: round(v, 4) for k, v in result.items()}



    training_args = TrainingArguments(
        per_device_train_batch_size=device_batch,
        gradient_accumulation_steps=gradient_steps,
        warmup_steps=1000,
        report_to=[],
        learning_rate=1e-4,
        lr_scheduler_type="cosine",
        num_train_epochs=10,
        logging_steps=500,
        optim="adamw_bnb_8bit",
        bf16=True,
        output_dir=f"results/{model_name_or_path.split('/')[-1]}_{LANGUAGE}_indication_5k",
        logging_strategy="steps",
        dataloader_num_workers=4,
        save_total_limit=3,
        do_eval=True,
        evaluation_strategy="steps",
        save_strategy="steps",
        save_steps=1_000,
        eval_steps=1_000,
        metric_for_best_model = 'eval_meteor',
        greater_is_better=True,
        load_best_model_at_end = True
    )

    trainer = SFTTrainer(
        model,
        packing=False, # pack samples together for efficient training
        max_seq_length=300, # maximum packed length
        args=training_args,
        train_dataset=processed_train_dataset.shuffle(),
        eval_dataset=processed_test_dataset.shuffle().select(range(eval_samples)),
        dataset_text_field='text',
        compute_metrics=compute_metrics,
        callbacks = [transformers.EarlyStoppingCallback(early_stopping_patience=3)]

    )
    trainer.train()
    trainer.save_model()