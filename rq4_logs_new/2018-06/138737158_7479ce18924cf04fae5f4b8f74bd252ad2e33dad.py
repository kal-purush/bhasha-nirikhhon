# -*- coding:utf-8 -*-
from __future__ import print_function, division
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pickle
import torch
import torch.nn as nn
from torch.autograd import Variable
import torch.nn.functional as F
import time
import math
#from keras.preprocessing.sequence import pad_sequences
from sklearn.cross_validation import train_test_split


# Functions to accomplish attention
def batch_matmul_bias(seq, weight, bias, nonlinearity=''):
    s = None
    bias_dim = bias.size()
    for i in range(seq.size(0)):
        _s = torch.mm(seq[i], weight)
        _s_bias = _s + bias.expand(bias_dim[0], _s.size()[0]).transpose(0,1)
        if nonlinearity == 'tanh':
            _s_bias = torch.tanh(_s_bias)
        _s_bias = _s_bias.unsqueeze(0)
        if s is None:
            s = _s_bias
        else:
            s = torch.cat((s, _s_bias), 0)
    return s.squeeze()


def batch_matmul(seq, weight, nonlinearity=''):
    s = None
    for i in range(seq.size(0)):
        _s = torch.mm(seq[i], weight)
        if nonlinearity == 'tanh':
            _s = torch.tanh(_s)
        _s = _s.unsqueeze(0)
        if s is None:
            s = _s
        else:
            s = torch.cat((s, _s), 0)
    return s.squeeze()


def attention_mul(rnn_outputs, att_weights):
    attn_vectors = None
    for i in range(rnn_outputs.size(0)):
        h_i = rnn_outputs[i]
        a_i = att_weights[i].unsqueeze(1).expand_as(h_i)
        h_i = a_i * h_i
        h_i = h_i.unsqueeze(0)
        if attn_vectors is None:
            attn_vectors = h_i
        else:
            attn_vectors = torch.cat((attn_vectors, h_i), 0)
    return torch.sum(attn_vectors, 0)


# tim attention model with bias
class AttentionTimRNN(nn.Module):
    def __init__(self, batch_size, num_tims, num_locs, embed_size_locs, embed_size_tims, tim_gru_hidden, bidirectional=True):
        super(AttentionTimRNN, self).__init__()
        self.batch_size = batch_size
        self.num_locs = num_locs
        self.num_tims = num_tims
        self.embed_size_tims = embed_size_tims
        self.embed_size_locs = embed_size_locs
        self.tim_gru_hidden = tim_gru_hidden
        self.bidirectional = bidirectional

        self.lookup_tim = nn.Embedding(num_tims, embed_size_tims)
        self.lookup_loc = nn.Embedding(num_locs, embed_size_locs)

        if bidirectional:
            self.tim_gru = nn.GRU(embed_size_tims + embed_size_locs, tim_gru_hidden, bidirectional=True)
            self.weight_W_tim = nn.Parameter(torch.Tensor(2 * tim_gru_hidden, 2 * tim_gru_hidden))
            self.bias_tim = nn.Parameter(torch.Tensor(2 * tim_gru_hidden, 1))
            self.weight_proj_tim = nn.Parameter(torch.Tensor(2 * tim_gru_hidden, 1))
        else:
            self.tim_gru = nn.GRU(embed_size_tims + embed_size_locs, tim_gru_hidden, bidirectional=False)
            self.weight_W_tim = nn.Parameter(torch.Tensor(tim_gru_hidden, tim_gru_hidden))
            self.bias_tim = nn.Parameter(torch.Tensor(tim_gru_hidden, 1))
            self.weight_proj_tim = nn.Parameter(torch.Tensor(tim_gru_hidden, 1))

        self.softmax_tim = nn.Softmax()
        self.weight_W_tim.data.uniform_(-0.1, 0.1)
        self.weight_proj_tim.data.uniform_(-0.1, 0.1)

    # def forward(self, embed, state_word):
    def forward(self, tim_seq, loc_seq, state_tim):
        # embeddings
        embedded_tim = self.lookup_tim(tim_seq.long())
        embedded_loc = self.lookup_loc(loc_seq.long())
        # not sure of the dim
        embedded = torch.cat((embedded_tim, embedded_loc), dim=2)

        # time level gru
        output_tim, state_tim = self.tim_gru(embedded, state_tim)
        tim_squish = batch_matmul_bias(output_tim, self.weight_W_tim, self.bias_tim, nonlinearity='tanh')
        if len(tim_squish.size()) < 3:
            tim_squish = tim_squish.unsqueeze(0)
        tim_attn = batch_matmul(tim_squish, self.weight_proj_tim)
        if len(tim_attn.size()) < 2:
            tim_attn = tim_attn.unsqueeze(0)
        tim_attn_norm = self.softmax_tim(tim_attn.transpose(1, 0))
        tim_attn_vectors = attention_mul(output_tim, tim_attn_norm.transpose(1, 0))
        return tim_attn_vectors.view(-1, tim_attn_vectors.size()[-2], tim_attn_vectors.size()[-1]), state_tim, tim_attn_norm

    def init_hidden(self):
        if self.bidirectional:
            return Variable(torch.zeros(2, self.batch_size, self.tim_gru_hidden))
        else:
            return Variable(torch.zeros(1, self.batch_size, self.tim_gru_hidden))


# day attention model with bias
class AttentionDayRNN(nn.Module):
    def __init__(self, batch_size, day_gru_hidden, tim_gru_hidden, embedding_size_day, bidirectional=True):
        super(AttentionDayRNN, self).__init__()
        self.batch_size = batch_size
        self.day_gru_hidden = day_gru_hidden
        self.tim_gru_hidden = tim_gru_hidden
        self.embedding_size_day = embedding_size_day
        # self.n_classes = n_classes
        # self.tim_gru_hidden = tim_gru_hidden
        self.bidirectional = bidirectional
        self.lookup_day = nn.Embedding(7, embedding_dim=embedding_size_day)

        if bidirectional:
            # self.day_gru = nn.GRU(2 * day_gru_hidden, day_gru_hidden, bidirectional=True)
            self.day_gru = nn.GRU(embedding_size_day + 2*tim_gru_hidden, day_gru_hidden, bidirectional=True)
            self.weight_W_day = nn.Parameter(torch.Tensor(2 * day_gru_hidden, 2 * day_gru_hidden))
            self.bias_day = nn.Parameter(torch.Tensor(2 * day_gru_hidden, 1))
            self.weight_proj_day = nn.Parameter(torch.Tensor(2 * day_gru_hidden, 1))
            # self.final_linear = nn.Linear(2 * sent_gru_hidden, n_classes)
        else:
            self.day_gru = nn.GRU(embedding_size_day + tim_gru_hidden, day_gru_hidden, bidirectional=False)
            self.weight_W_day = nn.Parameter(torch.Tensor(day_gru_hidden, day_gru_hidden))
            self.bias_day = nn.Parameter(torch.Tensor(day_gru_hidden, 1))
            self.weight_proj_day = nn.Parameter(torch.Tensor(day_gru_hidden, 1))
            # self.final_linear = nn.Linear(sent_gru_hidden, n_classes)

        self.softmax_day = nn.Softmax()
        self.weight_W_day.data.uniform_(-0.1, 0.1)
        self.weight_proj_day.data.uniform_(-0.1, 0.1)

    def forward(self, day_seq, day_vec, state_day):
        # embeddings
        embedded_day = self.lookup_day(day_seq.long())
        # not sure of the dim
        embedded = torch.cat((embedded_day, day_vec), dim=2)

        # time level gru
        output_day, state_day = self.day_gru(embedded, state_day)
        day_squish = batch_matmul_bias(output_day, self.weight_W_day, self.bias_day, nonlinearity='tanh')
        day_attn = batch_matmul(day_squish, self.weight_proj_day)
        day_attn_norm = self.softmax_day(day_attn.transpose(1, 0))
        day_attn_vectors = attention_mul(output_day, day_attn_norm.transpose(1, 0))
        return day_attn_vectors.view(-1, day_attn_vectors.size()[-2], day_attn_vectors.size()[-1]), state_day, day_attn_norm

    def init_hidden(self):
        if self.bidirectional:
            return Variable(torch.zeros(2, self.batch_size, self.day_gru_hidden))
        else:
            return Variable(torch.zeros(1, self.batch_size, self.day_gru_hidden))


# day attention model with bias
class AttentionUserRNN(nn.Module):
    def __init__(self, batch_size, user_gru_hidden, day_gru_hidden, week_nums, embedding_size_week, bidirectional=True):
        super(AttentionUserRNN, self).__init__()
        self.batch_size = batch_size
        self.user_gru_hidden = user_gru_hidden
        # self.n_classes = n_classes
        self.day_gru_hidden = day_gru_hidden
        self.bidirectional = bidirectional
        self.lookup_week = nn.Embedding(week_nums, embedding_size_week)
        self.embedding_size_week = embedding_size_week

        if bidirectional:
            self.user_gru = nn.GRU(embedding_size_week + 2*day_gru_hidden, user_gru_hidden, bidirectional=True)
            self.weight_W_user = nn.Parameter(torch.Tensor(2 * user_gru_hidden, 2 * user_gru_hidden))
            self.bias_user = nn.Parameter(torch.Tensor(2 * user_gru_hidden, 1))
            self.weight_proj_user = nn.Parameter(torch.Tensor(2 * user_gru_hidden, 1))
            # self.final_linear = nn.Linear(2 * user_gru_hidden, n_classes)
        else:
            self.user_gru = nn.GRU(embedding_size_week + day_gru_hidden, user_gru_hidden, bidirectional=False)
            self.weight_W_user = nn.Parameter(torch.Tensor(user_gru_hidden, user_gru_hidden))
            self.bias_user = nn.Parameter(torch.Tensor(user_gru_hidden, 1))
            self.weight_proj_user = nn.Parameter(torch.Tensor(user_gru_hidden, 1))
            # self.final_linear = nn.Linear(sent_gru_hidden, n_classes)
        self.softmax_user = nn.Softmax()
        self.weight_W_user.data.uniform_(-0.1, 0.1)
        self.weight_proj_user.data.uniform_(-0.1, 0.1)

    def forward(self, week_seq, week_vec, state_user):
        # embeddings
        embedded_week = self.lookup_week(week_seq.long())
        # not sure of the dim
        embedded = torch.cat((embedded_week, week_vec), dim=2)

        # user level gru
        output_user, state_user = self.user_gru(embedded, state_user)
        user_squish = batch_matmul_bias(output_user, self.weight_W_user, self.bias_user, nonlinearity='tanh')
        user_attn = batch_matmul(user_squish, self.weight_proj_user)
        user_attn_norm = self.softmax_user(user_attn.transpose(1, 0))
        user_attn_vectors = attention_mul(output_user, user_attn_norm.transpose(1, 0))
        return user_attn_vectors, state_user, user_attn_norm

    def init_hidden(self):
        if self.bidirectional:
            return Variable(torch.zeros(2, self.batch_size, self.user_gru_hidden))
        else:
            return Variable(torch.zeros(1, self.batch_size, self.user_gru_hidden))


class MLPNet(nn.Module):
    def __init__(self, input_size, hidden_size, bidirectional=True):
        super(MLPNet, self).__init__()
        if bidirectional:
            self.fc1 = nn.Linear(2 * input_size, hidden_size[0])
        else:
            self.fc1 = nn.Linear(input_size, hidden_size[0])
        self.fc2 = nn.Linear(hidden_size[0], hidden_size[1])

    def forward(self, x):
        out = F.relu(self.fc1(x))
        out = F.sigmoid(self.fc2(out))
        return out


def train_data(mini_batch, seq_week, seq_day, targets, tim_attn_model, day_attn_model, week_attn_model, mlp_model,
               tim_optimizer, day_optimizer, week_optimizer, mlp_optimizer, criterion):
    state_tim = tim_attn_model.init_hidden().cuda()
    state_day = day_attn_model.init_hidden().cuda()
    state_week = week_attn_model.init_hidden().cuda()
    _, max_weeks, max_days, _, batch_size, max_tims = mini_batch.size()
    tim_optimizer.zero_grad()
    day_optimizer.zero_grad()
    week_optimizer.zero_grad()
    mlp_optimizer.zero_grad()

    y_preds = []
    state_weeks = []
    for user in range(2):
        s1 = None
        for i in range(max_weeks):
            s2 = None
            for j in range(max_days):
                _s, state_tim, _ = tim_attn_model(mini_batch[user, i, j, 1, :, :].transpose(0, 1),
                                                  mini_batch[user, i, j, 0, :, :].transpose(0, 1), state_tim)
                if s2 is None:
                    s2 = _s
                else:
                    s2 = torch.cat((s2, _s), 0)
            _s, state_day, _ = day_attn_model(seq_day[user, i, :, :].transpose(0, 1), s2, state_day)
            if s1 is None:
                s1 = _s
            else:
                s1 = torch.cat((s1, _s), 0)
        y_pred_out, state_week_out, _ = week_attn_model(seq_week[user, :, :].transpose(0, 1), s1, state_week)
        y_preds.append(y_pred_out)
        state_weeks.append(state_week_out)

    y_pred = mlp_model(torch.cat((y_preds[0], y_preds[1]), dim=1))

    loss = criterion(y_pred.cuda(), targets)
    loss.backward()

    tim_optimizer.step()
    day_optimizer.step()
    week_optimizer.step()
    mlp_optimizer.step()

    return loss.data[0]


def get_predictions(val_tokens, seq_week, seq_day, tim_attn_model, day_attn_model, week_attn_model, mlp_model):
    state_tim = tim_attn_model.init_hidden().cuda()
    state_day = day_attn_model.init_hidden().cuda()
    state_week = week_attn_model.init_hidden().cuda()
    _, max_weeks, max_days, _, batch_size, max_tims = val_tokens.size()

    y_preds = []
    state_weeks = []
    for user in range(2):
        s1 = None
        for i in range(max_weeks):
            s2 = None
            for j in range(max_days):
                _s, state_tim, _ = tim_attn_model(val_tokens[user, i, j, 1, :, :].transpose(0, 1),
                                                  val_tokens[user, i, j, 0, :, :].transpose(0, 1), state_tim)
                if s2 is None:
                    s2 = _s
                else:
                    s2 = torch.cat((s2, _s), 0)
            _s, state_day, _ = day_attn_model(seq_day[user, i, :, :].transpose(0, 1), s2, state_day)
            if s1 is None:
                s1 = _s
            else:
                s1 = torch.cat((s1, _s), 0)
        y_pred_out, state_week_out, _ = week_attn_model(seq_week[user, :, :].transpose(0, 1), s1, state_week)
        y_preds.append(y_pred_out)
        state_weeks.append(state_week_out)

    y_pred = mlp_model(torch.cat((y_preds[0], y_preds[1]), dim=1))

    return y_pred


def pad_batch(mini_batch):
    mini_batch_size = len(mini_batch)
    tim_len = []
    day_len = []
    week_len = []
    for item in mini_batch:
        for user in item:
            week_len.append(len(user))
            for week in user:
                day_len.append(len(user[week]))
                for day in user[week]:
                    tim_len.append(len(user[week][day]))
    max_week_len = int(np.mean(week_len))
    max_day_len = int(np.mean(day_len))
    max_tim_len = int(np.mean(tim_len))
    main_matrix = np.zeros((mini_batch_size, 2, max_week_len, max_day_len, max_tim_len, 2), dtype=np.int)
    week_matrix = np.zeros((mini_batch_size, 2, max_week_len), dtype=np.int)
    day_matrix = np.zeros((mini_batch_size, 2, max_week_len, max_day_len), dtype=np.int)
    for i in range(main_matrix.shape[0]):
        for user in range(2):
            weeks = list(mini_batch[i][user].keys())
            for j in range(main_matrix.shape[1]):
                try:
                    days = list(mini_batch[i][user][weeks[j]].keys())
                    week_matrix[i, user, j] = weeks[j]
                    for k in range(main_matrix.shape[2]):
                        day_matrix[i, user, j, k] = days[k]
                        for p in range(main_matrix.shape[3]):
                                main_matrix[i, user, j, k, p, 0] = mini_batch[i][user][weeks[j]][days[k]][p][0]  # loc
                                main_matrix[i, user, j, k, p, 1] = mini_batch[i][user][weeks[j]][days[k]][p][1]  # time
                except IndexError:
                    pass
    return Variable(torch.from_numpy(main_matrix).permute(1, 2, 3, 5, 0, 4)), Variable(torch.from_numpy(week_matrix).permute(1, 0, 2)), Variable(torch.from_numpy(day_matrix).permute(1, 2, 0, 3))


def test_accuracy_mini_batch(tokens, seq_week, seq_day, labels, tim_attn, day_attn, week_attn, mlp):
    y_pred = get_predictions(tokens, seq_week, seq_day, tim_attn, day_attn, week_attn, mlp)
    _, y_pred = torch.max(y_pred, 1)
    correct = np.ndarray.flatten(y_pred.data.cpu().numpy())
    labels = np.ndarray.flatten(labels.data.cpu().numpy())
    num_correct = sum(correct == labels)
    return float(num_correct) / len(correct)


def test_accuracy_full_batch(tokens, labels, mini_batch_size, tim_attn, day_attn, week_attn, mlp):
    p = []
    l = []
    g = gen_minibatch(tokens, labels, mini_batch_size)
    for token, seq_week, seq_day, label in g:
        y_pred = get_predictions(tokens.cuda(), seq_week, seq_day, tim_attn, day_attn, week_attn, mlp)
        _, y_pred = torch.max(y_pred, 1)
        p.append(np.ndarray.flatten(y_pred.data.cpu().numpy()))
        l.append(np.ndarray.flatten(label.data.cpu().numpy()))
    p = [item for sublist in p for item in sublist]
    l = [item for sublist in l for item in sublist]
    p = np.array(p)
    l = np.array(l)
    num_correct = sum(p == l)
    return float(num_correct) / len(p)


def iterate_minibatches(inputs, targets, batchsize, shuffle=False):
    assert inputs.shape[0] == targets.shape[0]
    if shuffle:
        indices = np.arange(inputs.shape[0])
        np.random.shuffle(indices)
    for start_idx in range(0, inputs.shape[0] - batchsize + 1, batchsize):
        if shuffle:
            excerpt = indices[start_idx:start_idx + batchsize]
        else:
            excerpt = slice(start_idx, start_idx + batchsize)
        yield inputs[excerpt], targets[excerpt]


def gen_minibatch(tokens, labels, mini_batch_size, shuffle=True):
    for token, label in iterate_minibatches(tokens, labels, mini_batch_size, shuffle=shuffle):
        token, seq_week, seq_day = pad_batch(token)
        yield token.cuda(), seq_week.cuda(), seq_day.cuda(), Variable(torch.from_numpy(label), requires_grad=False).cuda()


def check_val_loss(val_tokens, val_labels, mini_batch_size, tim_attn_model, day_attn_model, week_attn_model, mlp_model, criterion):
    val_loss = []
    for token, label in iterate_minibatches(val_tokens, val_labels, mini_batch_size, shuffle=True):
        token, seq_week, seq_day = pad_batch(token)
        y_pred = get_predictions(token.cuda(), seq_week, seq_day, tim_attn_model, day_attn_model, week_attn_model, mlp_model)
        loss = criterion(y_pred.cuda(), Variable(torch.from_numpy(label), requires_grad=False).cuda())
        val_loss.append(loss.data[0])
    return np.mean(val_loss)


def timeSince(since):
    now = time.time()
    s = now - since
    m = math.floor(s / 60)
    s -= m * 60
    return '%dm %ds' % (m, s)


# Training
def train_early_stopping(mini_batch_size, X_train, y_train, X_test, y_test, tim_attn_model, day_attn_model, user_attn_model, mlp_model,
                         tim_attn_optimiser, day_attn_optimiser, user_attn_optimiser, mlp_optimizer, loss_criterion, num_epoch,
                         print_val_loss_every=1000, print_loss_every=50):
    start = time.time()
    loss_full = []
    loss_epoch = []
    accuracy_epoch = []
    accuracy_full = []
    epoch_counter = 0
    g = gen_minibatch(X_train, y_train, mini_batch_size)
    for i in range(1, num_epoch + 1):
        try:
            tokens, seq_week, seq_day, labels = next(g)
            loss = train_data(tokens, seq_week, seq_day, labels, tim_attn_model, day_attn_model, user_attn_model, mlp_model, tim_attn_optimiser,
                              day_attn_optimiser, user_attn_optimiser, mlp_optimizer, loss_criterion)
            acc = test_accuracy_mini_batch(tokens, seq_week, seq_day, labels, tim_attn_model, day_attn_model, user_attn_model, mlp_model)
            accuracy_full.append(acc)
            accuracy_epoch.append(acc)
            loss_full.append(loss)
            loss_epoch.append(loss)
            # print loss every n passes
            if i % print_loss_every == 0:
                print('Loss at %d minibatches, %d epoch,(%s) is %f' % (i, epoch_counter, timeSince(start), np.mean(loss_epoch)))
                print('Accuracy at %d minibatches is %f' % (i, np.mean(accuracy_epoch)))
            # check validation loss every n passes
            if i % print_val_loss_every == 0:
                val_loss = check_val_loss(X_test, y_test, mini_batch_size, tim_attn_model, day_attn_model, user_attn_model, mlp_model, loss_criterion)
                print('Average training loss at this epoch..minibatch..%d..is %f' % (i, np.mean(loss_epoch)))
                print('Validation loss after %d passes is %f' % (i, val_loss))
                if val_loss > np.mean(loss_full):
                    print(
                        'Validation loss is higher than training loss at %d is %f , stopping training!' % (i, val_loss))
                    print('Average training loss at %d is %f' % (i, np.mean(loss_full)))
        except StopIteration:
            epoch_counter += 1
            print('Reached %d epocs' % epoch_counter)
            print('i %d' % i)
            g = gen_minibatch(X_train, y_train, mini_batch_size)
            loss_epoch = []
            accuracy_epoch = []
    return loss_full


def train():
    # Loading the data
    friendship = pd.read_csv('../../../dataset/friendship.csv', header=None)
    with open('../../../dataset/traj_data.pkl', 'rb') as f:
        data = pickle.load(f)
    data_df = []
    for i in range(friendship.shape[0]):
        data_df.append([[data[friendship.iloc[i, 0]], data[friendship.iloc[i, 1]]], friendship.iloc[i, 2]])
    data_df = pd.DataFrame(data_df, columns=['tokens', 'rating'])

    # d = pd.read_json('../../../dataset/imdb_final.json')
    # d['rating'] = d['rating'] - 1
    # d = d[['tokens', 'rating']]

    X = data_df.tokens
    y = data_df.rating

    X_train, X_test, y_train, y_test = train_test_split(X.values, y.values, test_size=0.3, random_state=42)

    # Functions to train the model
    tim_gru_hidden = 100
    day_gru_hidden = 100
    user_gru_hidden = 100

    embed_size_locs = 100
    embed_size_tims = 100
    embedding_size_day = 100
    embedding_size_week = 100

    tim_attn = AttentionTimRNN(batch_size=64, num_tims=24, num_locs=40559, embed_size_locs=embed_size_locs,
                               embed_size_tims=embed_size_tims, tim_gru_hidden=tim_gru_hidden, bidirectional=True)
    day_attn = AttentionDayRNN(batch_size=64, day_gru_hidden=day_gru_hidden, tim_gru_hidden=tim_gru_hidden,
                               embedding_size_day=embedding_size_day, bidirectional=True)
    user_attn = AttentionUserRNN(batch_size=64, user_gru_hidden=user_gru_hidden, day_gru_hidden=day_gru_hidden,
                                 week_nums=133, embedding_size_week=embedding_size_week, bidirectional=True)
    mlp = MLPNet(input_size=2*user_gru_hidden, hidden_size=[100, 50], bidirectional=True)

    learning_rate = 1e-1
    momentum = 0.9
    tim_optmizer = torch.optim.SGD(tim_attn.parameters(), lr=learning_rate, momentum=momentum)
    day_optmizer = torch.optim.SGD(day_attn.parameters(), lr=learning_rate, momentum=momentum)
    user_optimizer = torch.optim.SGD(user_attn.parameters(), lr=learning_rate, momentum=momentum)
    mlp_optimizer = torch.optim.SGD(mlp.parameters(), lr=learning_rate, momentum=momentum)
    criterion = nn.NLLLoss()

    tim_attn.cuda()
    day_attn.cuda()
    user_attn.cuda()
    mlp.cuda()

    loss_full = train_early_stopping(64, X_train, y_train, X_test, y_test, tim_attn, day_attn, user_attn, mlp,
                                     tim_optmizer, day_optmizer, user_optimizer, mlp_optimizer,
                                     criterion, 5000, 1000, 50)

    # test_accuracy_full_batch(X_test, y_test, 64, tim_attn, day_attn, user_attn)
    #
    # test_accuracy_full_batch(X_train, y_train, 64, tim_attn, day_attn, user_attn)


if __name__ == '__main__':
    train()