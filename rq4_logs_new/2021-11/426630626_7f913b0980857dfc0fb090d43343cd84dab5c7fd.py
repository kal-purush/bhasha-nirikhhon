import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
import argparse
import h5py
import os


def parse_pr_pred(filepath):
    m_scores = {}
    n_scores = {}
    s_scores = {}
    with open(filepath, "r") as f:
        for line in f:
            if not line.startswith("Position"):
                split = line.strip().split("\t")
                position = split[0]
                position = int(position)
                m_scores[position] = float(split[1])
                n_scores[position] = float(split[3])
                s_scores[position] = float(split[5])
    output = {"m": m_scores, "n": n_scores, "s": s_scores}
    return output


def parse_all_preds(filepath):
    predictions = {}
    for subdir, dirs, files in os.walk(filepath):
        for file in files:
            path = os.path.join(subdir, file)
            identifier = get_identifier_from_path(path)
            prediction = parse_pr_pred(os.path.join(subdir, file))
            predictions[identifier] = prediction
    return predictions


def get_identifier_from_path(filepath):
    split_path = filepath.split("\\")
    identifier = split_path[-1].split(".")[0]
    return identifier


def calc_new_score(protein_score, residue_score):
    if protein_score > 0.5:
        if protein_score > 0.9:
            return residue_score + 0.1
        else:
            return residue_score + 0.05
    else:
        if protein_score < 0.1:
            return 0
        else:
            return residue_score - 0.05


def load_embeddings(filepath):
    embeddings = {}
    with h5py.File(filepath, 'r') as f:
        for new_identifier in f.keys():
            embeddings[f[new_identifier].attrs["original_id"]] = np.array(f[new_identifier])
    return embeddings


def write_output(pr_predictions, output_path):
    for prot_id in pr_predictions:
        with open(output_path + f"/{prot_id}_refined.bindPredict_out", "a+") as file:
            # check if the file es empty, if so write the header
            file.seek(0)
            if not len(file.read(100)) > 0:
                file.write("Position\tMetal.Proba\tMetal.Class\tNuclear.Proba\tNuclear.Class\tSmall.Proba\tSmall.Class\tAny.Class\n")
            for i in range(1,len(pr_predictions[prot_id]["m"])+1):
                line = f"{i}\t{pr_predictions[prot_id]['m'][i]}\t{'b' if pr_predictions[prot_id]['m'][i] > 0.5 else 'nb'}" \
                       f"\t{pr_predictions[prot_id]['n'][i]}\t{'b' if pr_predictions[prot_id]['n'][i] > 0.5 else 'nb'}\t" \
                       f"{pr_predictions[prot_id]['s'][i]}\t{'b' if pr_predictions[prot_id]['s'][i] > 0.5 else 'nb'}\t" \
                       f"{'b' if max(pr_predictions[prot_id]['m'][i], pr_predictions[prot_id]['n'][i], pr_predictions[prot_id]['s'][i]) > 0.5 else 'nb'}\n"
                file.write(line)


def combination_ensembl(model_dir, pr_predictions, embeddings, output_path):
    # load the models
    m_model = torch.load(model_dir + "/combination_m.pth")
    n_model = torch.load(model_dir + "/combination_n.pth")
    s_model = torch.load(model_dir + "/combination_s.pth")
    m_model.eval().to(device)
    n_model.eval().to(device)
    s_model.eval().to(device)

    for prot_id in pr_predictions:
        # load the embeddings and make it a tensor
        with torch.no_grad():
            embedding = torch.from_numpy(np.array(embeddings[prot_id])).to(device)
            embedding = embedding.type(torch.float32)
            m_prediction = m_model(embedding)
            n_prediction = n_model(embedding)
            s_prediction = s_model(embedding)

            # apply softmax to the predictions
            m_prediction = F.softmax(m_prediction, dim=0)
            n_prediction = F.softmax(n_prediction, dim=0)
            s_prediction = F.softmax(s_prediction, dim=0)

            produce_new_score_for_prot(m_prediction[1].item(), pr_predictions[prot_id]["m"])
            produce_new_score_for_prot(n_prediction[1].item(), pr_predictions[prot_id]["n"])
            produce_new_score_for_prot(s_prediction[1].item(), pr_predictions[prot_id]["s"])

    # write the output
    write_output(pr_predictions, output_path)


def produce_new_score_for_prot(probability, predictions):
    for position in predictions:
        old_score = predictions[position]
        predictions[position] = calc_new_score(probability, old_score)


def combination_old(model_dir_path, pr_predictions, embeddings, output_path):
    model = torch.load(model_dir_path + "/7_class_predictor.pth")
    model.eval().to(device)

    for prot_id in pr_predictions:
        with torch.no_grad():
            embedding = torch.from_numpy(np.array(embeddings[prot_id])).to(device)
            embedding = embedding.float()
            prediction = model(embedding)
            prediction = F.softmax(prediction, dim=0)

            # find the highest probability for each class
            m_probabilities = [prediction[0].item(), prediction[3].item(), prediction[4].item(), prediction[6].item()]
            n_probabilities = [prediction[1].item(), prediction[3].item(), prediction[5].item(), prediction[6].item()]
            s_probabilities = [prediction[2].item(), prediction[4].item(), prediction[5].item(), prediction[6].item()]

            m_probability = max(m_probabilities)
            n_probability = max(n_probabilities)
            s_probability = max(s_probabilities)

            # make the new scores
            produce_new_score_for_prot(m_probability, pr_predictions[prot_id]["m"])
            produce_new_score_for_prot(n_probability, pr_predictions[prot_id]["n"])
            produce_new_score_for_prot(s_probability, pr_predictions[prot_id]["s"])
    # write output
    write_output(pr_predictions, output_path)


def combination_three_class(model_dir_path, pr_predictions, embeddings, output_path):
    model = torch.load(model_dir_path + "/3_class_predictor.pth")
    model.eval().to(device)

    for prot_id in pr_predictions:
        with torch.no_grad():
            embedding = torch.from_numpy(np.array(embeddings[prot_id])).to(device)
            embedding = embedding.float()
            prediction = model(embedding)
            prediction = torch.sigmoid(prediction)

            # make the new scores
            produce_new_score_for_prot(prediction[0].item(), pr_predictions[prot_id]["m"])
            produce_new_score_for_prot(prediction[1].item(), pr_predictions[prot_id]["n"])
            produce_new_score_for_prot(prediction[2].item(), pr_predictions[prot_id]["s"])

    # write output
    write_output(pr_predictions, output_path)


class NeuralNet(nn.Module):
    def __init__(self, input_size, hidden_size, num_classes, dropout_rate_input=0.5, dropout_rate_hidden=0.3):
        super(NeuralNet, self).__init__()
        self.classifier = nn.Sequential(
            nn.Dropout(dropout_rate_input),   # input layer dropout
            nn.Linear(input_size, hidden_size),
            nn.Dropout(dropout_rate_hidden),   # hidden layer dropout
            nn.ReLU(),
            nn.Linear(hidden_size, num_classes)
        )

    def forward(self, x):
        out = self.classifier(x)
        # put activation function and softmax here if not using CrossEntropyLoss
        return out


if __name__ == '__main__':
    # find the appropriate device to run the code on
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    print(f"Using Device {device}")

    # parse the arguments
    parser = argparse.ArgumentParser(description="Predict ligand binding for all proteins that have an embedding in the embeddings file.")
    parser.add_argument("-e", "--embeddings", help="Filepath to embeddings file", required=True, nargs=1)
    parser.add_argument("-o", "--output_dir", help="Filepath to the output directory", required=True, nargs=1)
    parser.add_argument("-m", "--models", help="Filepath to the models directory", required=True, nargs=1)
    parser.add_argument("-r", "--residue", help="Filepath to the directory with the residue predictions", required=True, nargs=1)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-t", action="store_true", help="Indicates the use of the 3-class Predictor.")
    group.add_argument("-s", action="store_true", help="Indicates the use of the 7-class Predictor.")
    group.add_argument("-c", action="store_true", help="Indicated the use of the combination approach.")

    args = parser.parse_args()

    # load the mebeddings
    embeddings = load_embeddings(args.embeddings[0])

    # load the residue predictions
    residue_predictions = parse_all_preds(args.residue[0])

    if args.c:
        combination_ensembl(args.models[0], residue_predictions, embeddings, args.output_dir[0])
    elif args.t:
        combination_three_class(args.models[0], residue_predictions, embeddings, args.output_dir[0])
    elif args.s:
        combination_old(args.models[0], residue_predictions, embeddings, args.output_dir[0])