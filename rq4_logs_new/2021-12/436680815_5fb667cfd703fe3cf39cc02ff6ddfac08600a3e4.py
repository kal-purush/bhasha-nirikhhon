import numpy as np
import pandas as pd

import sys

sys.path.append('/scratch/user/rujieyin/OpenNE/src/openne')
sys.path.append('/scratch/user/rujieyin/OpenNE')

def EdgeSplitter(P_edge, N_edge, smp_ratio = 0.1, balanced = True):

    n_P_tt = P_edge.shape[0]
    n_N_tt = N_edge.shape[0]
    

    n_smp_P = int(n_P_tt*smp_ratio)
    if balanced:
        n_smp_N = n_smp_P
    else:
        n_smp_N = n_smp_P*10
    

    n_P_rest = n_P_tt - n_smp_P
    n_N_rest = N_edge.shape[0] - n_smp_N

    P_choice = np.random.choice(n_P_tt, n_smp_P, replace=False)
    N_choice = np.random.choice(n_N_tt, n_smp_N, replace=False)
    

    smp_P = P_edge[P_choice]
    smp_N = N_edge[N_choice]
    

    P_rest_idx = np.ones(n_P_tt)
    P_rest_idx[P_choice] = 0
    rest_P = P_edge[np.arange(n_P_tt)[P_rest_idx == 1]]

    N_rest_idx = np.ones(n_N_tt)
    N_rest_idx[N_choice] = 0
    rest_N = N_edge[np.arange(n_N_tt)[N_rest_idx == 1]]

    return smp_P, smp_N, rest_P, rest_N

def smp_label_trans(P_edge, N_edge):
    n_P = P_edge.shape[0]
    n_N = N_edge.shape[0]

    smp = np.concatenate((P_edge, N_edge), axis=0)
    label = np.concatenate((np.ones(n_P), np.zeros(n_N)), axis=0)

    return (smp, label)


########
graph_P = []
graph_N = []

with open("/scratch/user/rujieyin/OpenNE/data/neodti/mat_drug_protein_edgelist.txt", "r") as f:
    for line in f:
        line_split = line.strip().split()
        if line_split[2] == "1":
            graph_P.append([int(line_split[0]), int(line_split[1])])
        else:
            graph_N.append([int(line_split[0]), int(line_split[1])])

graph_P = np.array(graph_P)
graph_N = np.array(graph_N)



# smp_test_P, smp_test_N for testing
# graph_test_P, graph_test_N for testing embedding
smp_test_P, smp_test_N, graph_test_P, graph_test_N = EdgeSplitter(
    graph_P, graph_N, 0.1
)

# graph_train_P, graph_train_N for embedding
smp_P, smp_N, graph_train_P, graph_train_N = EdgeSplitter(
    graph_test_P, graph_test_N, 0.1
)

# model_selection_P, model_selection_N for model selection
# smp_train_clf_P, smp_train_clf_N for training the classifier
model_selection_P, model_selection_N, smp_train_clf_P, smp_train_clf_N = EdgeSplitter(smp_P, smp_N, 0.25)

########
from calc_embedding import calc_embedding

embedding_train = calc_embedding(graph_train_P, graph_train_N)

from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegressionCV
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import average_precision_score, precision_recall_curve
from sklearn.metrics import auc


# 1. link embeddings
def link_examples_to_features(link_examples, transform_node, binary_operator):
    return [
        binary_operator(transform_node(src), transform_node(dst))
        for src, dst in link_examples
    ]


# 2. training classifier
def train_link_prediction_model(
    link_examples, link_labels, get_embedding, binary_operator
):
    clf = link_prediction_classifier()
    link_features = link_examples_to_features(
        link_examples, get_embedding, binary_operator
    )
    clf.fit(link_features, link_labels)
    return clf


def link_prediction_classifier(max_iter=2000):
    lr_clf = LogisticRegressionCV(Cs=10, cv=10, scoring="roc_auc", max_iter=max_iter)
    return Pipeline(steps=[("sc", StandardScaler()), ("clf", lr_clf)])


# 3. and 4. evaluate classifier
def evaluate_link_prediction_model(
    clf, link_examples_test, link_labels_test, get_embedding, binary_operator
):
    link_features_test = link_examples_to_features(
        link_examples_test, get_embedding, binary_operator
    )
    score = evaluate_roc_auc(clf, link_features_test, link_labels_test)
    return score


def evaluate_roc_auc(clf, link_features, link_labels):
    predicted = clf.predict_proba(link_features)

    # check which class corresponds to positive links
    positive_column = list(clf.classes_).index(1)

    ###############
    precision, recall, thresholds = precision_recall_curve(link_labels, predicted[:, positive_column])
    # Use AUC function to calculate the area under the curve of precision recall curve
    auc_precision_recall = auc(recall, precision)


    print("auprc!!!", auc_precision_recall)

    return roc_auc_score(link_labels, predicted[:, positive_column])


def operator_hadamard(u, v):
    return u * v


def operator_l1(u, v):
    return np.abs(u - v)


def operator_l2(u, v):
    return (u - v) ** 2


def operator_avg(u, v):
    return (u + v) / 2.0


def run_link_prediction(binary_operator):
    clf = train_link_prediction_model(
        smp_label_trans(smp_train_clf_P, smp_train_clf_N)[0], smp_label_trans(smp_train_clf_P, smp_train_clf_N)[1], embedding_train, binary_operator
    )
    score = evaluate_link_prediction_model(
        clf,
        smp_label_trans(model_selection_P, model_selection_N)[0],
        smp_label_trans(model_selection_P, model_selection_N)[1],
        embedding_train,
        binary_operator,
    )

    return {
        "classifier": clf,
        "binary_operator": binary_operator,
        "score": score,
    }


binary_operators = [operator_hadamard, operator_l1, operator_l2, operator_avg]




results = [run_link_prediction(op) for op in binary_operators]
best_result = max(results, key=lambda result: result["score"])

print("Best result from" , best_result['binary_operator'].__name__)


q = pd.DataFrame(
    [(result["binary_operator"].__name__, result["score"]) for result in results],
    columns=("name", "ROC AUC score"),
).set_index("name")

print(q)

embedding_test = calc_embedding(graph_test_P, graph_test_N)

# smp_test_P, smp_test_N
test_score = evaluate_link_prediction_model(
    best_result["classifier"],
    smp_label_trans(smp_test_P, smp_test_N)[0],
    smp_label_trans(smp_test_P, smp_test_N)[1],
    embedding_test,
    best_result["binary_operator"],
)
print(
"ROC AUC score on test set using ", best_result['binary_operator'].__name__,' : ',test_score)
