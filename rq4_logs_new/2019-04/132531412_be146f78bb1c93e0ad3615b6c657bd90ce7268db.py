import os
import pickle
import numpy as np
from scipy.sparse import csr_matrix, coo_matrix, hstack, vstack

data_folder = "/Users/jingxixu/Desktop/sorted_data"

domains = ["apparel", "electronics", "office_products",
           "automotive", "gourmet_food", "outdoor_living",
           "baby", "grocery", "software",
           "beauty", "health_&_personal_care", "sports_&_outdoors",
           "books", "jewelry_&_watches", "musical_instruments",
           "camera_&_photo", "kitchen_&_housewares",
           "cell_phones_&_service", "magazines", "tools_&_hardware",
           "computer_&_video_games", "music", "toys_&_games",
           "dvd", "video"]

# not enough data
domains =  [d for d in domains if d not in ['office_products', 'musical_instruments', 'tools_&_hardware']]

def extract_review(line):
    pairs = line.split()
    review_dict = dict()
    for pair in pairs:
        word, count = pair.split(":")
        if count in ['negative', 'positive']:
            label = 1 if count == 'positive' else 0
        else:
            count = int(count)
            review_dict[word] = count
    return review_dict, label


if __name__ == "__main__":
    if not os.path.exists("domain_map.p"):
        domain_dict = dict(zip(domains, range(len(domains))))
        pickle.dump(domain_dict, open("domain_map.p", "wb"))
    else:
        domain_dict = pickle.load(open("domain_map.p", "rb"))

    reviews, labels, tasks = list(), list(), list()
    for d in domains:
        data_filepath = os.path.join(data_folder, d, "processed.review.balanced")
        with open(data_filepath) as f:
            l = f.readline()
            while l:
                review_dict, label = extract_review(l)
                reviews.append(review_dict)
                labels.append(label)
                tasks.append(d)
                l = f.readline()

    if not os.path.exists('word_map.p'):
        words = list()
        for r in reviews:
            words += list(r.keys())
        words = set(words)
        print("num of words {}".format(len(words)))
        word_dict = dict(zip(words, range(len(words))))
        pickle.dump(word_dict, open("word_map.p", "wb"))
    else:
        word_dict = pickle.load(open("word_map.p", "rb"))

    # construct COO sparse matrix
    # data_train, row_train, col_train, Y_train = [], [], [], []
    # data_test, row_test, col_test, Y_test = [], [], [], []
    domain_count = {d:0 for d in domains}
    data, row, col, Y = [], [], [], []

    for i, (r, d, l) in enumerate(zip(reviews, tasks, labels)):
        print(i)
        row += (len(r.keys())) * [i]
        for k, v in r.items():
            col += [word_dict[k]]
            data += [v]
        assert len(row) == len(col) == len(data)
        Y.append(l)
        domain_count[d] += 1

    # get overall X and Y
    X = coo_matrix((data, (row, col)), shape=(len(reviews), len(word_dict.keys())))
    X = hstack((np.array([domain_dict[t] for t in tasks]).reshape((-1, 1)), X)) # add task id
    X = X.tocsr()
    Y = np.array(Y).reshape(-1, 1)

    # get overall data dictionary
    data_dict = {}
    for d in domains:
        d_index = domain_dict[d]
        mask = (X[:, 0].A.ravel() == d_index)
        X_d = X[mask]
        Y_d = Y[mask]
        mask_positive = (Y_d == 1).ravel()
        mask_negative = (Y_d == 0).ravel()
        X_d_pos, Y_d_pos = X_d[mask_positive], Y_d[mask_positive]
        X_d_neg, Y_d_neg = X_d[mask_negative], Y_d[mask_negative]
        data_dict[d] = {'positive': X_d_pos, 'negative': X_d_neg}


    X_train = np.array([]).reshape(0, X.shape[1])
    Y_train = np.array([]).reshape(0, 1)
    X_test = np.array([]).reshape(0, X.shape[1])
    Y_test = np.array([]).reshape(0, 1)
    # get train and test
    for k, v in data_dict.items():
        X_pos = v['positive']
        X_neg = v['negative']
        X_train = vstack((X_train, X_pos[:50, :], X_neg[:50, :]))
        Y_train = np.vstack((Y_train, np.ones((50, 1), dtype=np.int), np.zeros((50, 1), dtype=np.int)))
        n_pos_test = min(150, X_pos.shape[0]-50)
        n_neg_test = min(150, X_neg.shape[0]-50)
        X_test = vstack((X_test, X_pos[50:50+n_pos_test, :], X_neg[50:50+n_neg_test, :]))
        Y_test = np.vstack((Y_test, np.ones((n_pos_test, 1), dtype=np.int), np.zeros((n_neg_test, 1), dtype=np.int)))

    k = int(X_train.shape[0]/100)
    fea = X_train.shape[1]
    pickle.dump([X_train, Y_train, X_test, Y_test, k, fea], open('sentiment.p', 'wb'))