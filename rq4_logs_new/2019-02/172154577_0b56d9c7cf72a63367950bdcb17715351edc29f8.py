from keras.models import Sequential
from keras.layers import Dense
from keras import regularizers
from sklearn.model_selection import StratifiedKFold
import numpy as np
import tensorflow as tf
from keras import backend as K
from sklearn import metrics
from keras.optimizers import Adam
from sklearn.utils import class_weight
from imblearn.over_sampling import SMOTE
from sklearn import tree, svm
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import make_scorer


def auc(y_true, y_pred):
    score, up_opt = tf.metrics.auc(y_true, y_pred)
    K.get_session().run(tf.local_variables_initializer())
    with tf.control_dependencies([up_opt]):
        score = tf.identity(score)
    return score


def roc_auc_score(y_true, y_pred):
    """ ROC AUC Score.
    Approximates the Area Under Curve score, using approximation based on
    the Wilcoxon-Mann-Whitney U statistic.
    Yan, L., Dodier, R., Mozer, M. C., & Wolniewicz, R. (2003).
    Optimizing Classifier Performance via an Approximation to the Wilcoxon-Mann-Whitney Statistic.
    Measures overall performance for a full range of threshold levels.
    Arguments:
        y_pred: `Tensor`. Predicted values.
        y_true: `Tensor` . Targets (labels), a probability distribution.
    """
    with tf.name_scope("RocAucScore"):
        pos = tf.boolean_mask(y_pred, tf.cast(y_true, tf.bool))
        neg = tf.boolean_mask(y_pred, ~tf.cast(y_true, tf.bool))
        pos = tf.expand_dims(pos, 0)
        neg = tf.expand_dims(neg, 1)
        # original paper suggests performance is robust to exact parameter choice
        gamma = 0.2
        p = 3
        difference = tf.zeros_like(pos * neg) + pos - neg - gamma
        masked = tf.boolean_mask(difference, difference < 0.0)
        return tf.reduce_sum(tf.pow(-masked, p))


def roc_auc_score_loss(y_true, y_pred):
    return roc_auc_score(y_true, y_pred)


class NeuralNetwork(object):

    def __init__(self, hidden_nodes, n_epochs):
        # Set number of nodes in input, hidden and output layers.
        self.hidden_nodes = hidden_nodes
        self.n_epochs = n_epochs
        self.model = None

    def train(self, X, Y):
        # Oversampling
        sm = SMOTE(random_state=42, ratio=0.25, k_neighbors=100)
        X, Y = sm.fit_sample(X, Y)
        # fix random seed for reproducibility
        seed = 7
        np.random.seed(seed)
        # define 10-fold cross validation
        kfold = StratifiedKFold(n_splits=10, shuffle=True, random_state=seed)
        best_auc = 0
        print("Valor de AUC para cada um dos conjuntos de validação dos 10 folds:")
        for train, test in kfold.split(X, Y):
            # create model
            model = Sequential()  # type: object
            model.add(
                Dense(self.hidden_nodes, input_dim=X.shape[1], kernel_initializer='normal',
                      kernel_regularizer=regularizers.l1(1), activation='relu'))
            model.add(Dense(1, kernel_initializer='normal', activation='sigmoid'))
            # Compile model
            model.compile(loss=roc_auc_score_loss, optimizer=Adam(lr=0.01), metrics=[auc])
            # Fit the model
            class_weights = class_weight.compute_class_weight('balanced',
                                                              np.unique(Y[train]),
                                                              Y[train])
            model.fit(X[train], Y[train], epochs=self.n_epochs, batch_size=128, verbose=0,
                      class_weight=dict(enumerate(class_weights)))
            # evaluate the model
            scores = model.evaluate(X[test], Y[test], verbose=0)
            print("AUC: %.5f" % scores[1])
            if scores[1] > best_auc:
                best_auc = scores[1];
                self.model = model

    def test_auc(self, X, Y):
        y_pred = self.model.predict(X)
        return metrics.roc_auc_score(Y, y_pred)


class DecisionTree(object):

    def __init__(self):
        # Set number of nodes in input, hidden and output layers.
        self.model = None

    def train(self, X, Y):
        # Oversampling
        sm = SMOTE(random_state=42, ratio=1.0, k_neighbors=100)
        X, Y = sm.fit_sample(X, Y)
        # fix random seed for reproducibility
        seed = 7
        np.random.seed(seed)
        parameters = {'max_depth': range(3, 20)}
        kfold = StratifiedKFold(n_splits=10, shuffle=True, random_state=42)
        scorers = {
            'auc_score': make_scorer(metrics.roc_auc_score)
        }
        refit_score = 'auc_score'
        decTreeClf = GridSearchCV(tree.DecisionTreeClassifier(), parameters, n_jobs=4, cv=kfold,
                                  refit=refit_score, scoring=scorers)
        decTreeClf.fit(X=X, y=Y)
        self.model = decTreeClf.best_estimator_
        print("Melhores parametros: ", decTreeClf.best_params_)
        print("AUC treino: ", decTreeClf.cv_results_['mean_train_auc_score'])
        print("AUC validacao: ", decTreeClf.cv_results_['mean_test_auc_score'])

    def test_auc(self, X, Y):
        y_pred = self.model.predict(X)
        return metrics.roc_auc_score(Y, y_pred)