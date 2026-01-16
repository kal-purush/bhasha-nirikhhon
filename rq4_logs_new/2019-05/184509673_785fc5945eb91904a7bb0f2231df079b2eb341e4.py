
import json
import pandas
import numpy as np
import matplotlib.pyplot as plt
from sklearn.utils.multiclass import unique_labels
from sklearn.model_selection import KFold
from sklearn.metrics import *
from sklearn.ensemble import RandomForestClassifier


class RandomForest:
    """This class was made to classify plants species given the leaf dataset using the Random Forests technique"""

    #   The class constructor opens, reads and split the .csv between features and labels

    def __init__(self, leaf_csv_path):
        self.leaf_dataset = pandas.read_csv(leaf_csv_path, header=None)
        self.leaf_features = np.array(self.leaf_dataset.values[:, 2:])
        self.leaf_labels = np.array(list(map(int, self.leaf_dataset.values[:, 0])))
        self.rf_args = []

    #   Arguments for the classifiers
    #   The arguments below will follow this order
    #   {'n_estimators': 100, 'criterion': 'gini', 'max_depth': None,
    #             'min_samples_split': 2, 'min_samples_leaf': 1,
    #             'min_weight_fraction_leaf': 0.0, 'max_features': 'auto',
    #             'max_leaf_nodes': None, 'min_impurity_decrease': 0.0,
    #             'min_impurity_split': None, 'bootstrap' : True,
    #             'oob_score': False, 'n_jobs': None, 'random_state': None,
    #             'verbose': 0, 'warm_start': False, 'class_weight': None}
    @staticmethod
    def generate_classifier_args(n_estimators, criterion, max_depth, min_samples_split,
                                 min_samples_leaf, min_weight_fraction_leaf, max_features,
                                 max_leaf_nodes, min_impurity_decrease, min_impurity_split,
                                 bootstrap, oob_score, n_jobs, random_state, verbose,
                                 warm_start, class_weight):

        return {'n_estimators': n_estimators, 'criterion': criterion, 'max_depth': max_depth,
                'min_samples_split': min_samples_split, 'min_samples_leaf': min_samples_leaf,
                'min_weight_fraction_leaf': min_weight_fraction_leaf, 'max_features': max_features,
                'max_leaf_nodes': max_leaf_nodes, 'min_impurity_decrease': min_impurity_decrease,
                'min_impurity_split': min_impurity_split, 'bootstrap' : bootstrap,
                'oob_score': oob_score, 'n_jobs': n_jobs, 'random_state': random_state,
                'verbose': verbose, 'warm_start': warm_start, 'class_weight': class_weight}

    def set_classifier_args(self, argv):
        self.rf_args = argv


    def run_classifiers(self, filename):
        #   Creating the 10-folds for cross validation
        kf = KFold(n_splits=10, shuffle=True, random_state=13785)

        json_outfile = open('../results/json/{filename}.json'.format(filename=filename), 'w+', encoding='UTF-8')
        fold_index = 1
        json_obj = {}
        for train_index, test_index in kf.split(self.leaf_features):
            json_obj['fold{index}'.format(index=fold_index)] = []
            features_train, features_test = self.leaf_features[train_index], self.leaf_features[test_index]
            labels_train, labels_test = self.leaf_labels[train_index], self.leaf_labels[test_index]

            for args in self.rf_args:
                classifier = RandomForestClassifier(**args)
                classifier.fit(features_train, labels_train)
                predicts = classifier.predict(features_test)

                json_obj['fold{index}'.format(index=fold_index)] \
                    .append({'args': args,
                             'metrics': {
                                 'accuracy': accuracy_score(labels_test, predicts),
                                 'f1_score': {
                                     'micro': f1_score(labels_test, predicts, average='micro'),
                                     'macro': f1_score(labels_test, predicts, average='macro'),
                                     'weighted': f1_score(labels_test, predicts, average='weighted')
                                 },
                                 'precision': {
                                     'micro': precision_score(labels_test, predicts, average='micro'),
                                     'macro': precision_score(labels_test, predicts, average='macro'),
                                     'weighted': precision_score(labels_test, predicts, average='weighted')
                                 }
                             }
                             })
            fold_index += 1

        json_outfile.write(json.dumps(json_obj, indent=True))
        json_outfile.close()

    @staticmethod
    # Compute confusion matrix
    def plot_confusion_matrix(y_true, y_pred, classes,
                              normalize=False,
                              title=None,
                              cmap=plt.cm.Blues):
        cm = confusion_matrix(y_true, y_pred)
        # Only use the labels that appear in the data
        classes = classes[unique_labels(y_true, y_pred)]
        if normalize:
            cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
            print("Normalized confusion matrix")
        else:
            print('Confusion matrix, without normalization')

        print(cm)

        fig, ax = plt.subplots()
        im = ax.imshow(cm, interpolation='nearest', cmap=cmap)
        ax.figure.colorbar(im, ax=ax)
        # We want to show all ticks...
        ax.set(xticks=np.arange(cm.shape[1]),
               yticks=np.arange(cm.shape[0]),
               # ... and label them with the respective list entries
               xticklabels=classes, yticklabels=classes,
               title=title,
               ylabel='True label',
               xlabel='Predicted label')

        # Rotate the tick labels and set their alignment.
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
                 rotation_mode="anchor")

        # Loop over data dimensions and create text annotations.
        fmt = '.2f' if normalize else 'd'
        thresh = cm.max() / 2.
        for i in range(cm.shape[0]):
            for j in range(cm.shape[1]):
                ax.text(j, i, format(cm[i, j], fmt),
                        ha="center", va="center",
                        color="white" if cm[i, j] > thresh else "black")
        fig.tight_layout()
        return ax

    def get_confusion_matrix(self, filename, args):
        kf = KFold(n_splits=10, shuffle=True, random_state=13785)

        for train_index, test_index in kf.split(self.leaf_features):
            features_train, features_test = self.leaf_features[train_index], self.leaf_features[test_index]
            labels_train, labels_test = self.leaf_labels[train_index], self.leaf_labels[test_index]


            classifier = RandomForestClassifier(**args)
            classifier.fit(features_train, labels_train)
            predicts = classifier.predict(features_test)

            plot_fig = RandomForest.plot_confusion_matrix(labels_test, predicts, np.unique(labels_test), title=filename)
            plot_fig.savefig('../graphs/{filename}.png'.format(filename=filename), dpi=300)
            plot_fig.show()