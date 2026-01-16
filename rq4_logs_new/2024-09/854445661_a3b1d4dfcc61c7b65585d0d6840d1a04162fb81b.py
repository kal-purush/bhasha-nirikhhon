# Author: Mattes Warning
import os

import pandas as pd
import torch
import argparse
import sys
import numpy as np
myDir = os.getcwd()
sys.path.append(myDir)

from pathlib import Path
path = Path(myDir)
a=str(path.parent.absolute())

sys.path.append(a)

from sklearn.metrics import classification_report

from model.Model import Model, Model2
from dataset.Dataset import MedMNIST2D
from torch.utils.data import DataLoader
import torchvision.transforms as transforms
from tqdm import tqdm

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

class_mappings = {
 'Actinic keratoses and intraepithelial carcinoma': 0,
 'Basal cell carcinoma': 1,
 'Benign keratosis-like lesions': 2,
 'Dermatofibroma': 3,
 'Melanoma': 4,
 'Melanocytic nevi': 5,
 'Vascular lesions': 6
}


def compute_class_accuracies(truth, preds):

    out_dict = {}

    for label in range(7):
        class_indices = truth == label
        class_predictions = preds[class_indices]

        acc = sum(class_predictions == label) / len(class_predictions)

        out_dict[label] = round(acc.item(), 2)

    return out_dict



def main(args):

    predictions_df = pd.read_csv("predictions.csv")
    predictions_df['Class Index'] = predictions_df['Predicted Class'].map(class_mappings)
    labels_df = pd.read_csv("test_labels.csv")

    predictions = predictions_df['Class Index'].to_numpy()
    ground_truth = labels_df["labels"].to_numpy()

    total_accuracy = round(np.sum(ground_truth == predictions) / len(ground_truth), 2)
    class_accuracies = compute_class_accuracies(ground_truth, predictions)

    accs_list = [total_accuracy]
    cal = list(class_accuracies.values())
    accs_list.extend(cal)

    names_list = ["Total"]
    cml = list(class_mappings.keys())
    names_list.extend(cml)

    acc_df = pd.DataFrame({"Class": names_list, "Accuracy": accs_list})
    acc_df.to_csv("Accuracies.csv")

    print(total_accuracy)
    print(class_accuracies)





if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", default="data/dermamnist.npz")
    parser.add_argument("--split", default="test")
    parser.add_argument("--eval_every", default=10)
    args = parser.parse_args()

    main(args)