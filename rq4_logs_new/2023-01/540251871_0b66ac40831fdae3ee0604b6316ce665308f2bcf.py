import types
from transformers import AutoTokenizer, T5ForConditionalGeneration
from reader import read_data_full
import torch
import scipy.stats as stats
import math
import numpy as np
import time


def create_model(learning_rate = 3e-4):
    res = types.SimpleNamespace()
    # NOTE
    # res.t5 = T5ForConditionalGeneration.from_pretrained("google/flan-t5-small")
    # res.toker = AutoTokenizer.from_pretrained("google/flan-t5-small")
    res.t5 = T5ForConditionalGeneration.from_pretrained("google/flan-t5-base")
    res.toker = AutoTokenizer.from_pretrained("google/flan-t5-base")
    res.opter = torch.optim.AdamW(res.t5.parameters(), learning_rate)
    res.t5.cuda()
    res.t5.train()
    return res

def prepared_ds(ds):
    res = []
    prefix = 'sentence ordering: '
    for ss, ls in ds:
        input_text = prefix + ''.join([f'<extra_id_{index}>. {s} ' for index, s in enumerate(ss)])
        label_text = ''.join([f'<extra_id_{index}> {l} ' for index, l in enumerate(ls)])
        label_text += f'<extra_id_{len(ls)}>'
        res.append((input_text, label_text))
    return res

def train(m, train_ds):
    # train
    train_ds_processed = prepared_ds(train_ds)
    for index, (input_text, label_text) in enumerate(train_ds_processed):
        if (index + 1) % 500 == 0:
            print(f'{index + 1} / {len(train_ds)}')
        input_ids = m.toker(input_text, return_tensors="pt").input_ids.cuda()
        labels = m.toker(label_text, return_tensors="pt").input_ids.cuda()
        loss = m.t5(input_ids=input_ids, labels=labels).loss
        loss.backward()
        m.opter.step()
        m.opter.zero_grad()
        m.t5.zero_grad()

@torch.no_grad()
def test(m, test_ds):
    ls = [labels for ss, labels in test_ds]
    prepared_test_ds = prepared_ds(test_ds)
    y_preds_y_trues = []
    for (input_text, _), y_true in zip(prepared_test_ds, ls):
        input_ids = m.toker(input_text, return_tensors="pt").input_ids.cuda()
        outputs = m.t5.generate(input_ids)
        label_text = m.toker.decode(outputs[0], skip_special_tokens=True)
        y_pred = [int(item) for item in label_text.split()]
        y_preds_y_trues.append((y_pred, y_true))
    return score_method([y_pred for y_pred, y_true in y_preds_y_trues], [y_true for y_pred, y_true in y_preds_y_trues])

    
def score_method(y_preds, y_trues):
    taus = []
    for y_pred, y_true in zip(y_preds, y_trues):
        if len(y_pred) != 5:
            print(f'len(y_pred) != 5, {y_pred}, {y_true}')
            taus.append(0)
        else:
            tau = stats.kendalltau(y_pred, y_true)[0]
            if math.isnan(tau):
                print(f'nan {y_pred}, {y_true}')
                taus.append(0)
            else:
                taus.append(tau)
    score = np.mean(taus)
    print(f'Tested score = {score}')
    return score


def small_script():
    start_time = time.time()
    m = create_model()
    train_ds, test_ds = read_data_full()
    for start in [0, 5000, 10000, 15000, 20000, 25000, 30000, 35000]:
        train(m, train_ds[start: start + 5000])
        print(test(m, test_ds[:1000]))
    print("--- %s seconds ---" % (time.time() - start_time))
    return m


        