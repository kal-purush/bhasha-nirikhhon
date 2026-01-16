# Classical library imports
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset
import random
import numpy as np
import os
import pandas as pd
from sklearn.metrics import confusion_matrix, f1_score, accuracy_score

from statistics import stdev, mean
# Assigning the seed value for the random function

SEED = 61096
random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)
torch.cuda.manual_seed(SEED)
os.environ['PYTHONHASHSEED'] = '0'

# Checking if GPU is available or not
if torch.cuda.is_available():
    torch.backends.cudnn.deterministic = True
    print("GPU runtime selected, GPU device name:", torch.cuda.get_device_name())
else:
  print("No GPU runtime, running on CPU mode")

device = torch.device('cuda:0' if torch.cuda.is_available() else 'cpu')

# Assigning learning rate for model and epochs
num_data = 1000
lr = 0.001
epochs = 100


def extract_data(fold):
    train_path = os.path.join(f"folder{fold}", "train.csv")
    test_path = f"folder{fold}/test.csv"
    features = ['CharLength', 'TreeNewFeature', 'nGramReputation_Alexa', 'REBenign', 'MinREBotnets', 'Entropy',
                'InformationRadius']

    train_set = pd.read_csv(train_path, header=None, encoding='utf-8')
    test_set = pd.read_csv(test_path, header=None, encoding='utf-8')

    x_train = train_set.iloc[:,:-1].to_numpy()
    y_train = train_set.iloc[:,-1].to_numpy()

    x_test = test_set.iloc[:, :-1].to_numpy()
    y_test = test_set.iloc[:,-1].to_numpy()

    return x_train, y_train, x_test, y_test


class Centralized_model(nn.Module):
    def __init__(self):
        super(Centralized_model, self).__init__()
        self.classifier= nn.Sequential(
            nn.Linear(7,32),
            nn.ReLU(),
            #qlayer,
            nn.Linear(32,16),
            nn.ReLU(),
            nn.Linear(16,4),
            nn.ReLU(),
            #qlayer,
            nn.Linear(4,4),
            nn.ReLU(),
            nn.Linear(4,64),
            nn.ReLU(),
            nn.Dropout(0.5),
            nn.Linear(64,32),
            nn.ReLU(),
            nn.Dropout(0.5),
            nn.Linear(32,4),
            nn.ReLU(),
            #qlayer,
            nn.Linear(4,1),
            nn.Sigmoid()
        )
    def forward(self,x):
        y_hat = self.classifier(x)
        return y_hat


batch_size = 32
num_folds = 5

# Define cost function
criterion = nn.BCELoss()

K = 5
models = []
train_loss_store = [[] for i in range(K)]
train_acc_store = [[] for i in range(K)]

test_loss_store = [[] for i in range(K)]
test_acc_store = [[] for i in range(K)]
overall_confusion_matrix_store = [[] for i in range(K)]
for fold in range(1, K + 1):
    print("For Fold:", fold)
    x_train, y_train, x_test, y_test = extract_data(fold)
    model = Centralized_model().to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    fold_conf_matrix = []
    for epoch in range(epochs):
        model.train()
        train_loss_collect = []
        true_labels = []
        pred_labels = []
        for i in range(0, len(x_train), batch_size):
            inputs = torch.tensor(x_train[i:i + batch_size]).to(torch.float32).to(device)
            targets = torch.tensor(y_train[i:i + batch_size]).to(torch.float32).to(device)
            outputs = model(inputs).flatten()
            optimizer.zero_grad()
            loss = criterion(outputs, targets)
            train_loss_collect.append(loss.item() * inputs.size(0))
            loss.backward()
            optimizer.step()
            predictions = torch.round(outputs)
            pred_labels.extend(predictions.detach().cpu().numpy())
            true_labels.extend(targets.cpu().numpy())
        accuracy = accuracy_score(true_labels, pred_labels)
        train_loss_store[fold - 1].append(mean(train_loss_collect))
        train_acc_store[fold - 1].append(accuracy)
        print(f"For epoch {epoch + 1}/{epochs}: Train Accuracy = {accuracy}, Train loss = {mean(train_loss_collect)}")

        # Testing Loop
        model.eval()
        test_loss_collect = []
        test_accuracy_collect = []
        test_pred_labels = []
        test_true_labels = []
        with torch.no_grad():

            for i in range(0, len(x_test), batch_size):
                inputs = torch.tensor(x_test[i:i + batch_size]).to(torch.float32).to(device)
                targets = torch.tensor(y_test[i:i + batch_size]).to(torch.float32).to(device)

                outputs = model(inputs).flatten()
                # Compute loss
                loss = criterion(outputs, targets)
                test_loss_collect.append(loss.item() * inputs.size(0))

                # Accuracy
                predicted_labels = torch.round(outputs)
                test_pred_labels.extend(predicted_labels.cpu().numpy())
                test_true_labels.extend(targets.cpu().numpy())
            test_acc = accuracy_score(test_true_labels, test_pred_labels)
            test_acc_store[fold - 1].append(test_acc)
            test_loss_store[fold - 1].append(mean(test_loss_collect))
            print(f"                      : Test Accuracy = {test_acc}, Test loss = {mean(test_loss_collect)}")
        fold_conf_matrix = confusion_matrix(test_true_labels, test_pred_labels)
    overall_confusion_matrix_store[fold - 1].append(fold_conf_matrix)
    models.append(model)
print(overall_confusion_matrix_store)