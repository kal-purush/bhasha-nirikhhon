import math
import random
import sys

import numpy as np
from collections import defaultdict, Counter
from itertools import combinations, chain
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import json
import tqdm

import torch
import pyro.infer
import pyro.optim
# from pyro.infer.autoguide import AutoDelta

import brmp

import transformers

from significance import mean_diff_conf_inter, mean_diff_p, cohens_d
from util import *


# FILENAME = 'data/all_71.csv'
# MAP_FILENAME = 'data/map_71.json'
# FILENAME = 'data/all_71_proper_pre.csv'
# MAP_FILENAME = 'data/map_71_proper_pre.json'
FILENAME = 'data/all_71_proper_pre2.csv'
MAP_FILENAME = 'data/map_71_proper_pre2.json'
# FILENAME = 'data/all.csv'
# MAP_FILENAME = 'data/map.json'
STIMULI_FILENAME = 'data/stimuli.csv'

runs = 10
# global_seed = 123
global_seed = None

n_ffnn_iter = 25
n_svi_iter = 1000
# n_warmup = 5
n_samples = 100
# n_chains = 1
# n_print = n_iter
# trace_len = 50
split_fracs = [0.84, 0.01, 0.15]  # train, dev, test


def summarize(fit, mappings):
    m = fit.marginals()  # qs=[0.001, 0.005, 0.01, 0.9, 0.95, 0.99])
    means = dict(zip(m.row_labels, torch.FloatTensor(m.array[:, 0])))
    sds = dict(zip(m.row_labels, torch.FloatTensor(m.array[:, 1])))
    n_effs = dict(zip(m.row_labels, torch.FloatTensor(m.array[:, -2])))
    result_means = {}
    result_sds = {}
    result_n_effs = {}
    interesting = {}
    for k, mean in sorted(means.items()):
        sd = sds[k]
        n_eff = n_effs[k]
        if k.startswith('sd_'):
            f_id, fixed = k[3:].split('__')
            fixed = fixed.replace(':', 'x')
            fixed_f_id = f'{fixed}:{f_id}'
            if len(f_id.split(':')) == 1 or mean > 0.1:
                interesting[fixed_f_id] = mean
                if mean > interesting.get(fixed_f_id, 0):
                    interesting[fixed_f_id] = mean
        elif k.startswith('b_'):
            f_id = k[2:]
            if abs(mean) > 0.1:
                interesting[f_id] = mean
            result_means[f_id] = mean
            result_sds[f_id] = sd
            result_n_effs[f_id] = n_eff
        elif k.startswith('r_'):
            f_id, i = k[2:].strip(']').split('[')
            i, fixed = i.split(',')
            fixed = fixed.replace(':', 'x')
            fixed_f_id = f'{fixed}:{f_id}'
            fs = f_id.split(':')
            _is = i.split('_')
            _is = list(map(int, _is))
            _is = tuple(map(torch.tensor, _is))
            # if fixed != 'intercept':
            #     f_id = fixed_f_id
            if fixed not in mappings:
                mappings[fixed] = {'v2i': {fixed: 0}, 'i2v': {'0': fixed}}
            if fixed_f_id not in result_means:
                ns = [len(mappings[_f]['v2i']) for _f in fs]
                result_means[fixed_f_id] = torch.zeros(*ns)
                result_sds[fixed_f_id] = torch.zeros(*ns)
                result_n_effs[fixed_f_id] = torch.zeros(*ns)

            result_means[fixed_f_id].index_put_(_is, mean)
            result_sds[fixed_f_id].index_put_(_is, sd)
            result_n_effs[fixed_f_id].index_put_(_is, n_eff)

    return result_means, result_sds, result_n_effs, interesting



class MLP(torch.nn.Module):
    def __init__(self, factors, out_size, emb_size=5, contrasts=None):
        super(MLP, self).__init__()

        self.factors = factors
        self.emb_size = emb_size
        self.in_size = len(list(filter(lambda x: x > 1, self.factors.values()))) * self.emb_size + \
                       len(list(filter(lambda x: x == 1, self.factors.values()))) + \
                       sum([abs(x) for x in self.factors.values() if x < 1])
        self.out_size = out_size
        self.hidden_sizes = [max(768, 4 * self.in_size), max(256, 2 * self.in_size), (self.in_size + self.out_size) // 2]
        self.contrasts = contrasts

        self.embeddings = torch.nn.ModuleDict()
        for name, siz in self.factors.items():
            if siz > 1:
                self.embeddings[name] = torch.nn.Embedding(siz, self.emb_size)
        self.layers = torch.nn.ModuleList()
        prev = self.in_size
        for siz in self.hidden_sizes:
            self.layers.append(torch.nn.Linear(prev, siz))
            prev = siz
        self.layers.append(torch.nn.Linear(prev, out_size))

        self.activation = torch.nn.GELU()
        self.inp_dropout = torch.nn.Dropout(0.1)
        self.dropout = torch.nn.Dropout(0.2)

        self.loss = torch.nn.BCEWithLogitsLoss()

    def forward(self, x):
        _in = self.inp_dropout(x.clone().to(next(self.parameters()).device))
        for layer in self.layers:
            out = layer(_in)
            _in = self.activation(self.dropout(out)).clone()
        return out

    def dfrow_to_inp(self, row):
        result = []
        for f, siz in sorted(self.factors.items()):
            idx = row[f]
            if siz < 1:
                t = self.contrasts[f][int(idx)]
            else:
                t = torch.tensor(idx)
                if siz > 1:
                    t = self.embeddings[f](t.long())
                else:
                    assert t.dtype in (torch.float, torch.double), (f, t)
            result.append(t.float().view(-1))
        result = torch.cat(result, dim=0)
        return result

    def train_model(self, data, obs, iter, dev_data=None, patience=1, optim=None, lr=1e-4, batch_size=1, seed=None):
        if optim is None:
            optim = torch.optim.AdamW(params=self.parameters(), lr=lr)

        best_dev_acc = 0
        plateau = 0
        data_len = len(data)
        with tqdm.tqdm(range(iter), unit_scale=True, desc='Training FFNN...') as pbar:
            for n in range(iter):
                epoch_loss = 0
                epoch_acc = 0
                data = data.sample(frac=1, random_state=seed)
                batch = []
                tgt_batch = []
                for i, (_, d) in enumerate(data.iterrows()):
                    x = self.dfrow_to_inp(d)
                    batch.append(x.unsqueeze(0))
                    tgt = torch.tensor(d[obs]).float().view(-1)
                    tgt_batch.append(tgt.unsqueeze(0))

                    if len(batch) == batch_size or i in (0, data_len - 1):
                        batch = torch.cat(batch, dim=0)
                        tgt_batch = torch.cat(tgt_batch, dim=0)

                        self.train()
                        optim.zero_grad()
                        out = self.forward(batch)
                        loss = self.loss(out.view(-1), tgt_batch.view(-1))
                        loss.backward()
                        optim.step()

                        batch = []
                        tgt_batch = []

                    epoch_loss += loss.item()
                    epoch_loss_norm = epoch_loss / (i + 1)
                    if (i + 1) % 100 == 0:
                        pbar.update(100 / data_len)

                        if dev_data is None:
                            dev_data = data.sample(frac=0.01)
                        probs = self.predict(dev_data)
                        pred = pred_from_probs(probs)
                        gold = torch.LongTensor(dev_data[obs].to_numpy()).view(-1)
                        acc = (pred == gold).float().mean().item()
                        epoch_acc += acc
                        epoch_acc_norm = epoch_acc / ((i + 1) / 100)

                        pbar.set_postfix(i=i,
                                         loss=loss.item(), epoch_loss=epoch_loss_norm,
                                         acc=acc, epoch_acc=epoch_acc_norm,
                                         best_dev_acc=best_dev_acc,
                                         dev_acc_plateau=f'{plateau_dev_acc:.3f}({plateau}<{patience})'
                                         if plateau > 0 else '--')
                if epoch_acc_norm > best_dev_acc:
                    best_dev_acc = epoch_acc_norm
                    plateau = 0
                else:
                    plateau_dev_acc = epoch_acc_norm
                    plateau += 1
                    if plateau > patience:
                        print(f'\nNo improvement on dev ({plateau_dev_acc:.3f}<{best_dev_acc:.3f}) for more than {patience} epochs. '
                              f'Ending training after {n} epochs.')
                        break

    def predict(self, data):
        self.eval()

        batch = []
        for i, d in data.iterrows():
            x = self.dfrow_to_inp(d)
            batch.append(x.unsqueeze(0))
        batch = torch.cat(batch, dim=0)
        result = self.forward(batch)
        return result.sigmoid().view(-1)


def gpt_sentence_prob(s, tgt_char, lm, tokenizer):
    tok_outp = tokenizer(s, return_tensors='pt')
    toks = tok_outp.input_ids
    length = toks.shape[1]
    diag_mask = torch.ones(length, length).triu().tril().bool()
    masked_toks = toks.expand(length, -1).where(~diag_mask, torch.ones_like(toks) * tokenizer.mask_token_id)
    masked_outps = lm(masked_toks)
    ps = masked_outps.logits[diag_mask].softmax(dim=-1).gather(-1, toks.squeeze().unsqueeze(-1)).detach()

    outps = lm(toks, output_hidden_states=True)
    tgt_idx = tok_outp.char_to_token(tgt_char)
    hiddens = [h.squeeze()[tgt_idx].detach() for h in outps.hidden_states]

    return ps.squeeze(), hiddens, tgt_idx


def standardize(data):
    mu = np.mean(data)
    sd = np.std(data)
    return [(x - mu) / sd for x in data], mu, sd


if __name__ == '__main__':

    global mappings, n_train, n_students, n_conditions, n_answers, n_prepositions, n_fxns, n_idioms, n_tasks, n_times, student_ids, item_ids, answer_ids, preposition_ids, construal_ids, idiom_ids, task_ids, time_ids, obs, ax1, ax2, ax3, ax4, ax5, ax6, ax7, ax8, ax9, ax10, ax11, ax12, ax13, ax14, loss_ax, t

    all_data = data = pd.read_csv(FILENAME).astype('category')
    with open(MAP_FILENAME) as f:
        mappings = json.load(f)

    print('all data len', len(data))

    # print(data.dtypes)

    del data['Unnamed: 0'], data['item_id'], data['Filler'], data['Rule Number']
    del mappings['item_id'], mappings['Filler'], mappings['Rule Number']
    n_students = len(mappings['student_id']['v2i'])
    n_conditions = len(mappings['condition_id']['v2i'])
    n_prepositions = len(mappings['prep_id']['v2i'])
    n_fxns = len(mappings['fxn_id']['v2i'])
    n_idioms = len(mappings['idiom_id']['v2i'])
    n_tasks = len(mappings['task_id']['v2i'])
    n_times = len(mappings['time_id']['v2i'])

    n = len(data)

    settings = [
        # ('base', ['condition_id', 'task_id', 'time_id', 'idiom_id']),
        # ('prep1', ['fxn_id']),
        # ('prep2', ['fxn_id', 'idiom_id']),
        # ('stud1', ['student_id']),
        # ('stud2', ['student_id', 'answer_id']),
        # ('prep-base', ['condition_id', 'task_id', 'time_id', 'fxn_id', 'idiom_id']),
        # ('stud-base', ['condition_id', 'answer_id', 'time_id', 'student_id', 'idiom_id']),
        # ('prep-stud-base', ['condition_id', 'time_id', 'task_id', 'fxn_id', 'idiom_id', 'student_id']),
        # ('gjt-prep-stud-answer-base', ['condition_id', 'time_id', 'answer_id', 'fxn_id', 'idiom_id', 'student_id']),
        # ('gjt-prep-stud-answer-ctrl', ['control_id', 'time_id', 'answer_id', 'fxn_id', 'idiom_id', 'student_id']),
        # ('pdt-prep-stud-ctrl', ['control_id', 'time_id', 'fxn_id', 'idiom_id', 'student_id']),
        # ('prep-stud-task-base', ['condition_id', 'time_id', 'task_id', 'fxn_id', 'idiom_id', 'student_id']),
        # ('gjt',  # 'gjt-ext',
        #  ['condition_id', 'time_id', 'fxn_id', 'idiom_id', 'student_id']),
        ('gjt-ext',
         ['condition_id', 'time_id', 'fxn_id', 'idiom_id', 'student_id', 'answer_id', 'lm_tgt_p', 'lm_avg_p']),
        ('gjt-ext_no-p',
         ['condition_id', 'time_id', 'fxn_id', 'idiom_id', 'student_id', 'answer_id']),
        ('gjt-ext_no-ans',
         ['condition_id', 'time_id', 'fxn_id', 'idiom_id', 'student_id', 'lm_tgt_p', 'lm_avg_p']),
        ('gjt-ext_no-stud',
         ['condition_id', 'time_id', 'fxn_id', 'idiom_id', 'answer_id', 'lm_tgt_p', 'lm_avg_p']),
        ('gjt-ext_no-prep',
         ['condition_id', 'time_id', 'student_id', 'answer_id', 'lm_tgt_p', 'lm_avg_p']),
        ('gjt-ext_no-interv',
         ['fxn_id', 'idiom_id', 'student_id', 'answer_id', 'lm_tgt_p', 'lm_avg_p']),
        # # ('gjt-lm', ['lm_hidden', 'condition_id', 'time_id', 'fxn_id', 'idiom_id', 'student_id', 'answer_id']),
        # # ('gjt-irt', ['lm_hidden', 'condition_id', 'time_id', 'fxn_id', 'idiom_id', 'student_id', 'answer_id']),
        # ('pet',  # 'pdt-prep-stud-base',
        #  ['condition_id', 'time_id', 'fxn_id', 'idiom_id', 'student_id']),
        ('all',  # 'prep-stud-answer-base',
         ['condition_id', 'time_id', 'answer_id', 'fxn_id', 'idiom_id', 'student_id']),
        ('all_no-stud',  # 'prep-stud-answer-base',
         ['condition_id', 'time_id', 'answer_id', 'fxn_id', 'idiom_id']),
        ('all_no-prep',  # 'prep-stud-answer-base',
         ['condition_id', 'time_id', 'answer_id', 'student_id']),
        ('all_no-ans',  # 'prep-stud-answer-base',
         ['condition_id', 'time_id', 'fxn_id', 'idiom_id', 'student_id']),
        ('all_no-interv',  # 'prep-stud-answer-base',
         ['answer_id', 'fxn_id', 'idiom_id', 'student_id']),
        #
        ('pet',  # 'prep-stud-answer-base',
         ['condition_id', 'time_id', 'fxn_id', 'idiom_id', 'student_id']),
        ('pet_no-stud',  # 'prep-stud-answer-base',
         ['condition_id', 'time_id', 'fxn_id', 'idiom_id']),
        ('pet_no-prep',  # 'prep-stud-answer-base',
         ['condition_id', 'time_id', 'student_id']),
        ('pet_no-interv',  # 'prep-stud-answer-base',
         ['fxn_id', 'idiom_id', 'student_id']),
    ]

    plot_data = {}
    signi = {}

    for setting_name, factors_to_include in settings:
        contrasts = None
        fixed_eff = '1'
        if 'lm_tgt_p' in factors_to_include:
            fixed_eff += ' + lm_tgt_p'
        if 'lm_avg_p' in factors_to_include:
            fixed_eff += ' + lm_avg_p'
            if 'lm_tgt_p' in factors_to_include:
                fixed_eff += ' + lm_tgt_p:lm_avg_p'

        interaction_factors = [fac for fac in factors_to_include if
                               fac not in {'student_id', 'lm_tgt_p', 'lm_avg_p', 'lm_hidden'}]

        formula = f'outcomes ~ {fixed_eff}'
        if setting_name == 'gjt-irt':
            formula += ' + (1 || student_id:fxn_id:idiom_id:answer_id) + (1 || condition_id:time_id) + (1 || lm_hidden)'
        else:
            if 'student_id' in factors_to_include:
                formula += f' + ({fixed_eff} || student_id)'
                for factor in interaction_factors:
                    if factor not in ('time_id', 'condition_id'):
                        formula += f' + ({fixed_eff} || student_id:{factor})'

            if 'lm_hidden' in factors_to_include:
                formula += ' + (1 || lm_hidden)'

            if interaction_factors:
                interaction_ns = list(range(1, len(interaction_factors) + 1))
                random_effs = [":".join(tup) for tup in get_tuples(interaction_factors, interaction_ns)]
                formula += ' + ' + ' + '.join(f'({fixed_eff} || {rand_e})' for rand_e in random_effs)

        print()
        print('\n SETTING NAME:')
        print(setting_name)
        print('\n FACTORS:')
        print(factors_to_include)
        print('\n FORMULA:')
        print(formula)

        gjt_data = all_data[all_data['task_id'] == mappings['task_id']['v2i']['GJT']].copy()
        print(f'task=GJT data len', len(gjt_data))
        pdt_data = all_data[all_data['task_id'] == mappings['task_id']['v2i']['PD']].copy()
        print(f'task=PD data len', len(pdt_data))

        if any('lm' in factor for factor in factors_to_include):
            roberta = transformers.RobertaForMaskedLM.from_pretrained('roberta-base')
            tokenizer = transformers.RobertaTokenizerFast.from_pretrained('roberta-base')

            stimuli_df = pd.read_csv(STIMULI_FILENAME)
            stimuli_dict = {}
            probs = {}
            hiddens = []
            for i, d in tqdm.tqdm(list(stimuli_df.iterrows()), desc='Getting LM probs and hiddens...'):
                number, answer, sentence, target = d['Number'], d['Answer'], d['Sentence'], d['Target']
                sentence = sentence.strip()
                stimuli_dict[number, answer] = sentence, target
                ps, hs, idx = gpt_sentence_prob(sentence, target, roberta, tokenizer)
                probs[number, answer] = ps, idx

                if 'lm_hidden' not in mappings:
                    mappings['lm_hidden'] = defaultdict(dict)
                mappings['lm_hidden']['v2i'][f'{number}-{answer}'] = i
                mappings['lm_hidden']['i2v'][str(i)] = f'{number}-{answer}'
                hiddens.append(hs[-1].view(1, -1).detach())

            data_lm_tgt_probs = []
            data_lm_avg_probs = []
            data_hiddens = []
            pretest_responses = Counter()
            posttest_responses = Counter()
            for i, d in gjt_data.iterrows():
                answer = mappings['answer_id']['i2v'][str(int(d['answer_id']))] == 'TRUE'
                try:
                    number = int(mappings['Number']['i2v'][str(int(d['Number']))])
                    response_correctness = mappings['outcomes']['i2v'][str(int(d['outcomes']))]
                    response = (response_correctness == 'RIGHT' and answer is True) or (response_correctness == 'WRONG' and answer is False)
                    ps, idx = probs[number, answer]
                except (KeyError, ValueError):
                    __task = mappings['task_id']['i2v'][str(int(d['task_id']))]
                    assert __task == "PD", __task
                    data_lm_tgt_probs.append(None)
                    data_lm_avg_probs.append(None)
                    data_hiddens.append(None)
                else:
                    if mappings['time_id']['i2v'][str(int(d['time_id']))] == 'PRE':
                        pretest_responses[number, answer, response] += 1
                    if mappings['time_id']['i2v'][str(int(d['time_id']))] == 'POST' and \
                            mappings['condition_id']['i2v'][str(int(d['condition_id']))] != 'CONTROL':
                        posttest_responses[number, answer, response] += 1
                    not_idx_mask = [i != idx for i in range(len(ps))]
                    data_lm_tgt_probs.append(float(ps[idx]))  # * 2 - 1)  #  * 100))
                    data_lm_avg_probs.append(float(ps[not_idx_mask].log().mean().exp()))  # * 2 - 1)  #  * 100))
                    data_hiddens.append(mappings['lm_hidden']['v2i'][f'{number}-{answer}'])


            # print('SENTENCE ANSWER TGT_P AVG_P RESP')
            for number, answer in sorted(stimuli_dict):
                sentence, _ = stimuli_dict[number, answer]
                ps, idx = probs[number, answer]
                ps_true, idx_true = probs[number, True]
                ps_false, idx_false = probs[number, False]
                expected1 = 'OK' if ps_true[idx_true] > ps_false[idx_false] else '??'
                expected2 = 'OK' if ps_true.mean() > ps_false.mean() else '??'
                pre_resp = pretest_responses[number, answer, True] / (
                            pretest_responses[number, answer, True] + pretest_responses[number, answer, False])
                post_resp = posttest_responses[number, answer, True] / (
                            posttest_responses[number, answer, True] + posttest_responses[number, answer, False])
                # print(expected1, expected2, sentence, answer,
                #       f'{pre_resp * 100:.2f}%', f'{post_resp * 100:.2f}%', f'{ps[idx] * 100:.2f}%', f'{ps.mean() * 100:.2f}%')

            lm_tgt_p, old_tgt_mu, old_tgt_sd = standardize(data_lm_tgt_probs)
            lm_avg_p, old_avg_mu, old_avg_sd = standardize(data_lm_avg_probs)
            print(
                f'lm_tgt_p was N(mu={old_tgt_mu}, sd={old_tgt_sd}), standardized to N(mu={np.mean(lm_tgt_p)}, sd={np.std(lm_tgt_p)})')
            print(
                f'lm_avg_p was N(mu={old_avg_mu}, sd={old_avg_sd}), standardized to N(mu={np.mean(lm_avg_p)}, sd={np.std(lm_avg_p)})')

            gjt_data['lm_tgt_p'] = lm_tgt_p
            gjt_data['lm_avg_p'] = lm_avg_p
            gjt_data['lm_hidden'] = data_hiddens
            gjt_data['lm_hidden'] = gjt_data['lm_hidden'].astype('category')

        if 'gjt' in setting_name:
            data = gjt_data
        elif 'pdt' in setting_name:
            data = pdt_data
        else:
            data = all_data

        factor_sizes = {x: (hiddens[0].numel() * -1 if x == 'lm_hidden' else (len(mappings[x]['v2i']) if x in mappings else 1)) for x in factors_to_include}

        for N in range(runs):

            SEED = global_seed if global_seed is not None else N  # random.randint(1, 9999)
            print(f'\n\nN={N}, SEED={SEED}')

            random.seed(SEED)
            pyro.set_rng_seed(SEED)
            pyro.clear_param_store()

            data = data.sample(frac=1, random_state=SEED)
            train, dev, test = train_test_split(data, split_fracs, SEED)
            n_train = len(train)
            n_test = len(test)

            gold = torch.LongTensor(test['outcomes'].to_numpy()).view(-1)
            gold_train = torch.LongTensor(train['outcomes'].to_numpy()).view(-1)

            rand_pred_train = torch.randint_like(gold_train, 0, 2)
            rand_pred = torch.randint_like(gold, 0, 2)
            rand_all_train_accs = (rand_pred_train == gold_train).float()
            rand_overall_train_acc = rand_all_train_accs.mean()
            rand_all_accs = (rand_pred == gold).float()
            rand_overall_acc = rand_all_accs.mean()

            print(f'Rand BL overall train accuracy: {rand_overall_train_acc * 100:.2f}% '
                  f'(pred: {rand_pred_train.float().mean() * 100:.2f}%, gold: {gold_train.float().mean() * 100:.2f}%)')
            print(f'Rand BL overall accuracy: {rand_overall_acc * 100:.2f}% '
                  f'(pred: {rand_pred.float().mean() * 100:.2f}%, gold: {gold.float().mean() * 100:.2f}%)')

            maj = (gold_train.float().mean() >= 0.5).long()
            maj_pred_train = torch.ones_like(gold_train) * maj
            maj_pred = torch.ones_like(gold) * maj
            maj_all_train_accs = (maj_pred_train == gold_train).float()
            maj_overall_train_acc = maj_all_train_accs.mean()
            maj_all_accs = (maj_pred == gold).float()
            maj_overall_acc = maj_all_accs.mean()

            print(f'Maj BL overall train accuracy: {maj_overall_train_acc * 100:.2f}% '
                  f'(pred: {maj_pred_train.float().mean() * 100:.2f}%, gold: {gold_train.float().mean() * 100:.2f}%)')
            print(f'Maj BL overall accuracy: {maj_overall_acc * 100:.2f}% '
                  f'(pred: {maj_pred.float().mean() * 100:.2f}%, gold: {gold.float().mean() * 100:.2f}%)')

            if 'lm_hidden' in factors_to_include:
                contrasts = {'lm_hidden': torch.cat(hiddens, dim=0)}

            ffnn = MLP(factors=factor_sizes, out_size=1, contrasts=contrasts)
            ffnn.train_model(data=train, dev_data=dev, patience=3, obs='outcomes',
                             iter=n_ffnn_iter, batch_size=50, lr=1e-3, seed=SEED)

            ffnn_probs_train = ffnn.predict(train)
            ffnn_pred_train = pred_from_probs(ffnn_probs_train)
            ffnn_all_train_accs = (ffnn_pred_train == gold_train).float()
            ffnn_overall_train_acc = ffnn_all_train_accs.mean()
            print(f'FFNN overall train accuracy: {ffnn_overall_train_acc * 100:.2f}% '
                  f'(pred: {ffnn_pred_train.float().mean() * 100:.2f}%, gold: {gold_train.float().mean() * 100:.2f}%)')

            ffnn_probs = ffnn.predict(test)
            ffnn_pred = pred_from_probs(ffnn_probs)
            ffnn_all_accs = (ffnn_pred == gold).float()
            ffnn_overall_acc = ffnn_all_accs.mean()
            print(f'FFNN overall accuracy: {ffnn_overall_acc * 100:.2f}% '
                  f'(pred: {ffnn_pred.float().mean() * 100:.2f}%, gold: {gold.float().mean() * 100:.2f}%)')


            # # formula += ' + ' + ' + '.join((f'(1 | {":".join(tup)})' for tup in get_tuples(['student_id', 'answer_id'], [2])))
            optim = pyro.optim.AdamW({'lr': 5e-2})  # , 'betas': [0.8, 0.99]})
            autoguide = pyro.infer.autoguide.AutoNormal

            model_and_data = brmp.brm(formula, data, family=brmp.family.Bernoulli,
                                      priors=[
                                          brmp.priors.Prior(('b',), brmp.family.Normal(0., 10.)),
                                          brmp.priors.Prior(('sd',), brmp.family.HalfNormal(100.)),
                                      ],
                                      contrasts=None if contrasts is None else {k: v.numpy() for k, v in contrasts.items()}
                                      )
            pyro.clear_param_store()

            train_prior = model_and_data.prior(num_samples=n_samples, seed=SEED)

            probs_prior_train = predict(train, train_prior, what='sample')['outcomes'].mean(dim=0)
            pred_prior_train = pred_from_probs(probs_prior_train).view(-1)
            all_prior_train_accs = (pred_prior_train == gold_train).float()
            overall_prior_train_acc = all_prior_train_accs.mean()
            print(f'BRM prior overall train accuracy: {overall_prior_train_acc * 100:.2f}% '
                  f'(pred: {pred_prior_train.float().mean() * 100:.2f}%, gold: {gold_train.float().mean() * 100:.2f}%)')

            prior_probs = predict(test, train_prior, what='sample')['outcomes'].mean(dim=0)
            prior_pred = pred_from_probs(prior_probs).view(-1)
            all_prior_accs = (prior_pred == gold).float()
            overall_prior_acc = all_prior_accs.mean()
            print(f'BRM prior overall accuracy: {overall_prior_acc * 100:.2f}% '
                  f'(pred: {prior_pred.float().mean() * 100:.2f}%, gold: {gold.float().mean() * 100:.2f}%)')

            train_fit = model_and_data.svi(df=train, iter=n_svi_iter, num_samples=n_samples, seed=SEED, optim=optim,
                                           autoguide=autoguide)

            probs_train = predict(train, train_fit, what='sample')['outcomes'].mean(dim=0)
            pred_train = pred_from_probs(probs_train).view(-1)
            all_train_accs = (pred_train == gold_train).float()
            overall_train_acc = all_train_accs.mean()
            print(f'BRM overall train accuracy: {overall_train_acc * 100:.2f}% '
                  f'(pred: {pred_train.float().mean() * 100:.2f}%, gold: {gold_train.float().mean() * 100:.2f}%)')

            probs = predict(test, train_fit, what='sample')['outcomes'].mean(dim=0)
            pred = pred_from_probs(probs).view(-1)
            all_accs = (pred == gold).float()
            overall_acc = all_accs.mean()
            print(f'BRM overall accuracy: {overall_acc * 100:.2f}% '
                  f'(pred: {pred.float().mean() * 100:.2f}%, gold: {gold.float().mean() * 100:.2f}%)')

            model_names = ['BRM', 'FFNN', 'Maj BL', 'Rand BL']
            model_train_preds = [pred_train, ffnn_pred_train, maj_pred_train, rand_pred_train]
            model_preds = [pred, ffnn_pred, maj_pred, rand_pred]
            zipped_train = list(zip(model_names, model_train_preds))
            zipped = list(zip(model_names, model_preds))
            for i, (nm1, pr1) in enumerate(zipped_train):
                for nm2, pr2 in zipped_train[i+1:]:
                    print(f'Agreement {nm1}-{nm2} (train): {(pr1 == pr2).float().mean() * 100:.2f}')
            for i, (nm1, pr1) in enumerate(zipped):
                for nm2, pr2 in zipped[i+1:]:
                    print(f'Agreement {nm1}-{nm2}: {(pr1 == pr2).float().mean() * 100:.2f}')

            # exit(0)

            pyro.clear_param_store()
            with open(f'brmp_pyro_model_{setting_name}.py', 'w') as f:
                f.write(brmp.pyro_codegen.genmodel(model_and_data.model.desc))

            fit = model_and_data.svi(iter=n_svi_iter, num_samples=n_samples, seed=SEED, optim=optim, autoguide=autoguide)
            print('\nALL MARGINALS')
            print(fit.marginals())
            print()

            means, sds, n_effs, interesting = summarize(fit=fit, mappings=mappings)
            setting_signi = {}
            setting_plot_data = {}
            print('SIGNIFICANCE')
            for name_i, name in enumerate(interesting):
                print()
                setting_signi[name] = defaultdict(dict)
                print(setting_name)
                if means[name].numel() == 1:
                    diff_name = f'{name} > 0'
                    mu, sd, n_eff = means[name], sds[name], n_effs[name]
                    conf_inter = mean_diff_conf_inter(mu, sd, n_samples, 0, 0, 0, q=0.95)
                    eff_size = cohens_d(mu, sd, n_samples, 0, 0, 0)
                    p_val = mean_diff_p(mu, sd, n_samples, 0, 0, 0)
                    setting_signi[name][0][0] = {'p': p_val, 'd': eff_size, 'ci': conf_inter}
                    print(name, f'mu={mu:.2f}', f'sd={sd:.2f}', f'n_eff={n_eff:.2f}')
                    print(diff_name,
                          f'Cohen\'s d={eff_size:.4f}',
                          'p is NaN' if math.isnan(p_val) else f'p={p_val:.4f}' if p_val >= 0.0001 else 'p<0.0001',
                          '***' if p_val < 0.001 else '**' if p_val < 0.01 else '*' if p_val < 0.05 else '--')
                    # visualize([mu], [name], sds=[sd], ax=ax, color=name_i)
                    setting_plot_data[name] = {'mu': [mu.item()], 'sd': [sd.item()], 'name': [name]}
                    continue
                print(f'{name} max sd', f'{interesting[name]:.2f}')
                # plt.show()
                idxs = sds[name].nonzero().tolist()
                fs = name.split(':')
                itr = sorted(zip(means[name].view(-1), idxs, sds[name].view(-1), n_effs[name].view(-1)), reverse=True)
                plot_mus, plot_names, plot_sds = [], [], []
                for i, vals1 in enumerate(itr):
                    mu, idx, sd, n_eff = vals1
                    idx_name = ':'.join([f"{mappings[f]['i2v'][str(_idx)]}" for _idx, f in zip(idx, fs[1:])])
                    plot_mus.append(mu.item())
                    plot_names.append(idx_name)
                    plot_sds.append(sd.item())
                    if 'student_id' in name or 'lm_hidden' in name:
                        continue
                    print(f'{name}={idx_name}', f'mu={mu:.2f}', f'sd={sd:.2f}', f'n_eff={n_eff:.2f}')
                    for vals2 in itr[i+1:]:
                        (mu1, idx1, sd1, n1), (mu2, idx2, sd2, n2) = sorted([vals1, vals2], reverse=True)
                        idx_diff = sum([int(i2 != i1) for i1, i2 in zip(idx1, idx2)])
                        if idx2 != idx1 and mu1 != mu2 and idx_diff == 1:
                            idx1_name = ';'.join([f"{f}={mappings[f]['i2v'][str(i1)]}" for i1, f in zip(idx1, fs[1:])])
                            idx2_name = ';'.join([f"{f}={mappings[f]['i2v'][str(i2)]}" for i2, f in zip(idx2, fs[1:])])
                            diff_name = f'{idx1_name} > {idx2_name}'
                            conf_inter = mean_diff_conf_inter(mu1, sd1, n_samples, mu2, sd2, n_samples, q=0.95)
                            eff_size = cohens_d(mu1, sd1, n_samples, mu2, sd2, n_samples)
                            p_val = mean_diff_p(mu1, sd1, n_samples, mu2, sd2, n_samples)
                            setting_signi[name][idx1_name][idx2_name] = {'p': p_val, 'd': eff_size, 'ci': conf_inter}
                            setting_signi[name][idx2_name][idx1_name] = {'p': p_val, 'd': eff_size, 'ci': conf_inter}
                            print(diff_name,
                                  f'{mu1:.3f} +- {sd1:.3f} > {mu2:.3f} +- {sd2:.3f}',
                                  f'diff={mu1 - mu2:.3f}',
                                  f'CI={conf_inter[0]:.3f};{conf_inter[1]:.3f}',
                                  f'Cohen\'s d={eff_size:.3f}',
                                  'p is NaN' if math.isnan(p_val) else f'p={p_val:.4f}' if p_val >= 0.0001 else 'p<0.0001',
                                  '***' if p_val < 0.001 else '**' if p_val < 0.01 else '*' if p_val < 0.05 else '--')
                    print()
                # visualize(plot_mus, plot_names, sds=plot_sds, ax=ax, color=name_i)
                setting_plot_data[name] = {'mu': plot_mus, 'sd': plot_sds, 'name': plot_names}
                print()
                # print()
            plot_data[setting_name] = setting_plot_data
            signi[setting_name] = setting_signi

    try:
        with open(f'plot_data_{"_".join(plot_data.keys())}.json', 'w') as f:
            json.dump(plot_data, f)
    except Exception as e:
        print('failed to write plot data', e, file=sys.stderr)
        pass

    try:
        with open('signi_data.json', 'w') as f:
            json.dump(signi, f)
    except Exception as e:
        print('failed to write signi data', e, file=sys.stderr)
        pass

    # if 'pet' in plot_data and 'gjt' in plot_data:
    #     _, ax1 = plt.subplots(1)
    #     for plot_factor in ['intercept:idiom_id', 'intercept:condition_id:time_id', 'intercept:time_id']:
    #         pet_d = plot_data['pet'][plot_factor]
    #         gjt_d = plot_data['gjt'][plot_factor]
    #         plot_factor = plot_factor.replace('intercept:', '')
    #         if plot_factor == 'condition_id:time_id':
    #             gjt_mus, gjt_sds, gjt_labels = [], [], []
    #             pet_mus, pet_sds, pet_labels = [], [], []
    #             l2gjt_ms = {l: (m, s) for m, s, l in zip(gjt_d['mu'], gjt_d['sd'], gjt_d['name'])}
    #             l2pet_ms = {l: (m, s) for m, s, l in zip(pet_d['mu'], pet_d['sd'], pet_d['name'])}
    #             for l in ['SM:DELAYED', 'RM:DELAYED', 'CM:DELAYED', 'CONTROL:DELAYED',
    #                       'SM:POST', 'RM:POST', 'CM:POST', 'CONTROL:POST']:
    #                 gjt_m, gjt_s = l2gjt_ms[l]
    #                 pet_m, pet_s = l2pet_ms[l]
    #                 l = l.replace('DELAYED', 'DLY').replace('CONTROL', 'CTRL')
    #                 gjt_mus.append(gjt_m)
    #                 gjt_sds.append(gjt_s)
    #                 gjt_labels.append(l)
    #                 pet_mus.append(pet_m)
    #                 pet_sds.append(pet_s)
    #                 pet_labels.append(l)
    #         else:
    #             gjt_mus, gjt_sds, gjt_labels = gjt_d['mu'], gjt_d['sd'], gjt_d['name']
    #             pet_mus, pet_sds, pet_labels = pet_d['mu'], pet_d['sd'], pet_d['name']
    #         visualize(gjt_mus, gjt_labels, sds=gjt_sds, ax=ax1, marker='^', alpha=0.9)
    #         visualize(pet_mus, pet_labels, sds=pet_sds, ax=ax1, marker='o', alpha=0.5)
    #
    # _, ax3 = plt.subplots(1)
    # if 'all' in plot_data:
    #     _, ax2 = plt.subplots(1)
    #     for m, s, l in zip(plot_data['all']['intercept:student_id:answer_id']['mu'],
    #                        plot_data['all']['intercept:student_id:answer_id']['sd'],
    #                        plot_data['all']['intercept:student_id:answer_id']['name']):
    #         stud_x, stud_xerr, stud_y, stud_yerr, stud_c = [], [], [], [], []
    #         if 'TRUE' in l:
    #             stud_y.append(m)
    #             stud_yerr.append(s)
    #         if 'FALSE' in l:
    #             stud_x.append(m)
    #             stud_xerr.append(s)
    #         if 'DESCRIBE' in l:
    #             stud_c.append(m)
    #     ax2.errorbar(x=stud_x, y=stud_y, xerr=stud_xerr, yerr=stud_yerr, linestyle='', elinewidth=0.5)
    #     ax2.scatter(x=stud_x, y=stud_y, c=stud_c, marker='.', cmap='cool')
    #
    #     for plot_factor in ['intercept:time_id:answer_id', 'intercept:time_id:fxn_id:answer_id']:
    #         # ax3
    #         pass
    #
    # if 'gjt' in plot_data:
    #     for plot_factor in ['lm_tgt_p:lm_avg_p:answer_id:idiom_id', 'lm_tgt_p:answer_id:fxn_id:idiom_id',
    #                         'lm_avg_p:answer_id:fxn_id:idiom_id']:
    #         # ax3
    #         pass
    # # ax.tick_params(labelsize=9)
    # # plt.savefig('fig.pgf')
    # plt.show()