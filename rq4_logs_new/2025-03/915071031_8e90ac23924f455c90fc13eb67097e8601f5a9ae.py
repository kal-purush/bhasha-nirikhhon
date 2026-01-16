import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter
import argparse

def analysis(args):
    df = pd.read_csv(args.folder + "/results.csv")

    amino_acids = "ACDEFGHIKLMNPQRSTVWY"

    wrong_aa_count_0 = Counter()
    wrong_aa_count_1 = Counter()

    # Iterate through the dataset

    # columns are: sequence,neq values,pred,true label
    for seq, pred_classes, true_classes in zip(df["sequence"], df["pred"], df["true label"]):
        true_classes = eval(true_classes)  # Convert string list to actual list
        pred_classes = eval(pred_classes)  # Convert string list to actual list
        seq = list(seq)
        for aa, true_c, pred_c in zip(seq, true_classes, pred_classes):
            if true_c != pred_c:  # If prediction is incorrect
                if true_c == 0:
                    wrong_aa_count_0[aa] += 1
                elif true_c == 1:
                    wrong_aa_count_1[aa] += 1


    # Plot the histogram
    x = np.arange(len(amino_acids))
    width = 0.4

    plt.figure(figsize=(12, 6))
    plt.bar(x - width/2, [wrong_aa_count_0[aa] for aa in amino_acids], width, label="Class 0")
    plt.bar(x + width/2, [wrong_aa_count_1[aa] for aa in amino_acids], width, label="Class 1")

    # Labels and title
    plt.xticks(x, amino_acids)
    plt.xlabel("Amino Acids")
    plt.ylabel("Counts of Wrong Predictions")
    plt.title("Amino Acid Distribution in Wrongly Predicted Classes")
    plt.legend()
    plt.show()
    plt.savefig(args.folder + "/wrong_predictions.png")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--folder", type=str, required=True)
    args = parser.parse_args()
    analysis(args)


