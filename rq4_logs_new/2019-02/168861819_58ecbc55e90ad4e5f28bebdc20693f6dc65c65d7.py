import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.svm import SVC, LinearSVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import learning_curve, GridSearchCV, ShuffleSplit, train_test_split
from sklearn.metrics import accuracy_score
from timeit import default_timer as timer
from sklearn.preprocessing import MinMaxScaler


def load_data(data_path):
    data = pd.read_csv(data_path)
    y = data.iloc[:, -1]
    X = data.iloc[:, :-1]
    return X, y


def split_data(X, y, test_size):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size)
    return X_train, X_test, y_train, y_test


# function from Scikit learn url:
# https://scikit-learn.org/stable/auto_examples/model_selection/plot_learning_curve.html#sphx-glr-auto-examples-model-selection-plot-learning-curve-py
def plot_learning_curve(estimator, title, X, y, ylim=None, cv=None,
                        n_jobs=None, train_sizes=np.linspace(.1, 1.0, 5)):
    """
    Generate a simple plot of the test and training learning curve.

    Parameters
    ----------
    estimator : object type that implements the "fit" and "predict" methods
        An object of that type which is cloned for each validation.

    title : string
        Title for the chart.

    X : array-like, shape (n_samples, n_features)
        Training vector, where n_samples is the number of samples and
        n_features is the number of features.

    y : array-like, shape (n_samples) or (n_samples, n_features), optional
        Target relative to X for classification or regression;
        None for unsupervised learning.

    ylim : tuple, shape (ymin, ymax), optional
        Defines minimum and maximum yvalues plotted.

    cv : int, cross-validation generator or an iterable, optional
        Determines the cross-validation splitting strategy.
        Possible inputs for cv are:
          - None, to use the default 3-fold cross-validation,
          - integer, to specify the number of folds.
          - :term:`CV splitter`,
          - An iterable yielding (train, test) splits as arrays of indices.

        For integer/None inputs, if ``y`` is binary or multiclass,
        :class:`StratifiedKFold` used. If the estimator is not a classifier
        or if ``y`` is neither binary nor multiclass, :class:`KFold` is used.

        Refer :ref:`User Guide <cross_validation>` for the various
        cross-validators that can be used here.

    n_jobs : int or None, optional (default=None)
        Number of jobs to run in parallel.
        ``None`` means 1 unless in a :obj:`joblib.parallel_backend` context.
        ``-1`` means using all processors. See :term:`Glossary <n_jobs>`
        for more details.

    train_sizes : array-like, shape (n_ticks,), dtype float or int
        Relative or absolute numbers of training examples that will be used to
        generate the learning curve. If the dtype is float, it is regarded as a
        fraction of the maximum size of the training set (that is determined
        by the selected validation method), i.e. it has to be within (0, 1].
        Otherwise it is interpreted as absolute sizes of the training sets.
        Note that for classification the number of samples usually have to
        be big enough to contain at least one sample from each class.
        (default: np.linspace(0.1, 1.0, 5))
    """
    plt.figure()
    plt.title(title)
    if ylim is not None:
        plt.ylim(*ylim)
    plt.xlabel("Training examples")
    plt.ylabel("Score")
    train_sizes, train_scores, test_scores = learning_curve(
        estimator, X, y, cv=cv, n_jobs=n_jobs, train_sizes=train_sizes)
    train_scores_mean = np.mean(train_scores, axis=1)
    train_scores_std = np.std(train_scores, axis=1)
    test_scores_mean = np.mean(test_scores, axis=1)
    test_scores_std = np.std(test_scores, axis=1)
    plt.grid()

    plt.fill_between(train_sizes, train_scores_mean - train_scores_std,
                     train_scores_mean + train_scores_std, alpha=0.1,
                     color="r")
    plt.fill_between(train_sizes, test_scores_mean - test_scores_std,
                     test_scores_mean + test_scores_std, alpha=0.1, color="g")
    plt.plot(train_sizes, train_scores_mean, 'o-', color="r",
             label="Training score")
    plt.plot(train_sizes, test_scores_mean, 'o-', color="g",
             label="Cross-validation score")

    plt.legend(loc="best")
    return plt


# SET THESE PARAMETERS TO DETERMINE WHICH MODELS TO RUN

# NOTE: Setting use_best_params to False and data_set to 'loan' will result in very slow runtimes
use_best_params = True  # Set to True to skip Grid Search on parameters and use previously found best parameters
data_set = 'loan'  # Wine data set is 'wine' and is 1,600 rows, Loan data set is 'loan' and is 30,000 rows

# CROSS VALIDATION
param_grids = {'tree':
                   [{'criterion': ['gini', 'entropy'],
                     'splitter': ['best', 'random'],
                     'min_samples_leaf':[1, 2, 4, 5, 6],
                     'min_samples_split': [2, 3, 4, 5]
                     }],
               'neural': [{'hidden_layer_sizes': [(100,)],
                           'activation': ['relu', 'identity', 'logistic', 'tanh'], #'identity', 'logistic', 'tanh',
                           'alpha': [0.0001, 0.001, 0.01],
                           'solver': ['adam'],
                           'learning_rate_init': [0.001, 0.01],
                           'max_iter': [2000],
                           }],
               'boost': [{'n_estimators': [50, 25, 75, 100],
                          'learning_rate': [1, 0.5, 0.75, 0.25]
                          }],
               'rbf-svm': [{'C': [1, 100],  # just add zeros
                        'kernel': ['rbf'], # rbf
                        }],
                'linear-svm': [{'C': [1, 100],  # just add zeros
                        'kernel': ['linear'], # rbf
                        }],
               'knn': [{'n_neighbors': range(1, 101, 3)}]
               }

# Plot learning curves
if data_set is 'wine':
    data_path = 'winequality-red.csv'
else:
    data_path = 'UCI_Credit_Card.csv'

# Load CSV data
X, y = load_data(data_path)

# Cross validation with 100 iterations to get smoother mean test and train
# score curves, each time with 20% data randomly selected as a validation set.
cv = ShuffleSplit(n_splits=100, test_size=0.2, random_state=0)  # used for everything except SVM
cv_svm = ShuffleSplit(n_splits=10, test_size=0.2, random_state=0)  # used for SVM to reduce training time

title = "Learning Curves Decision Tree"
if use_best_params:
    if data_set is 'wine':
        clf = DecisionTreeClassifier(criterion='entropy', min_samples_split=2, min_samples_leaf=1, splitter='best')
    else:
        clf = DecisionTreeClassifier(criterion='gini', min_samples_leaf=6, min_samples_split=3, splitter='best')
else:
    # Cross validation to get best estimators
    algorithm = DecisionTreeClassifier()
    clf = GridSearchCV(algorithm, param_grids['tree'], cv=cv, iid=False, n_jobs=-1)

# Train Model
X_train, X_test, y_train, y_test = split_data(X, y, 0.2)

train_time = None
if use_best_params:
    start = timer()
    clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start
else:
    clf.fit(X_train, y_train)

if use_best_params:
    estimator = clf
else:
    estimator = clf.best_estimator_

# Plot Learning Curve
plot_learning_curve(estimator, title, X, y, cv=cv, n_jobs=-1)

# Final test on held out training data
y_pred = estimator.predict(X_test)
acc = accuracy_score(y_pred, y_test)

# Check Training Time If used cross validation
if not use_best_params:
    train_clf = DecisionTreeClassifier(criterion=clf.best_params_['criterion'],
                                       splitter=clf.best_params_['splitter'],
                                       min_samples_leaf=clf.best_params_['min_samples_leaf'],
                                       min_samples_split=clf.best_params_['min_samples_split'])
    start = timer()
    train_clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start

# Print Results
print("Decision Tree:")
if not use_best_params:
    print(clf.best_params_.items())  # shows best params selected
print("Train Time: {:10.6f}s".format(train_time))
print("Accuracy: {:3.4f}%".format(acc))
# ##################################################################################

title = "Learning Curves AdaBoosted Tree"
if use_best_params:
    if data_set is 'wine':
        clf = AdaBoostClassifier(n_estimators=25, learning_rate=0.25)
    else:
        clf = AdaBoostClassifier(n_estimators=25, learning_rate=0.75)
else:
    # Cross validation to get best estimators
    algorithm = AdaBoostClassifier()
    clf = GridSearchCV(algorithm, param_grids['boost'], cv=cv, iid=False, n_jobs=-1)

# Train Model
X_train, X_test, y_train, y_test = split_data(X, y, 0.2)

train_time = None
if use_best_params:
    start = timer()
    clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start
else:
    clf.fit(X_train, y_train)

if use_best_params:
    estimator = clf
else:
    estimator = clf.best_estimator_

# Plot Learning Curve
plot_learning_curve(estimator, title, X, y, cv=cv, n_jobs=-1)

# Final test on held out training data
y_pred = estimator.predict(X_test)
acc = accuracy_score(y_pred, y_test)

# Check Training Time If used cross validation
if not use_best_params:
    train_clf = AdaBoostClassifier(n_estimators=clf.best_params_['n_estimators'],
                                   learning_rate=clf.best_params_['learning_rate'])
    start = timer()
    train_clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start

# Print Results
print("AdaBoosted Tree:")
if not use_best_params:
    print(clf.best_params_.items())  # shows best params selected
print("Train Time: {:10.6f}s".format(train_time))
print("Accuracy: {:3.4f}%".format(acc))
# ##################################################################################

title = "Learning Curves Neural Network"
if use_best_params:
    if data_set is 'wine':
        clf = MLPClassifier(max_iter=2000, solver='adam', alpha=0.0001, activation='tanh', learning_rate_init=0.001)
    else:
        clf = MLPClassifier(max_iter=2000, solver='adam', alpha=0.0001, learning_rate_init=0.001, activation='logistic')
else:
    # Cross validation to get best estimators
    algorithm = MLPClassifier()
    clf = GridSearchCV(algorithm, param_grids['neural'], cv=cv, iid=False, n_jobs=-1)

# Train Model
X_train, X_test, y_train, y_test = split_data(X, y, 0.2)

train_time = None
if use_best_params:
    start = timer()
    clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start
else:
    clf.fit(X_train, y_train)

if use_best_params:
    estimator = clf
else:
    estimator = clf.best_estimator_

# Plot Learning Curve
plot_learning_curve(estimator, title, X, y, cv=cv, n_jobs=-1)

# Final test on held out training data
y_pred = estimator.predict(X_test)
acc = accuracy_score(y_pred, y_test)

# Check Training Time If used cross validation
if not use_best_params:
    train_clf = MLPClassifier(activation=clf.best_params_['activation'],
                              alpha=clf.best_params_['alpha'],
                              learning_rate_init=clf.best_params_['learning_rate_init'],
                              max_iter=2000)
    start = timer()
    train_clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start

    # For nueral network, graph training iterations to accuracy
    # plt.figure()
    # plt.ylabel("Loss")
    # plt.xlabel("Epochs")
    # plt.title('Neural Network Loss vs Epochs')
    # plt.plot(train_clf.loss_curve_)
    # plt.show()

# Print Results
print("Neural Network:")
if not use_best_params:
    print(clf.best_params_.items())  # shows best params selected
print("Train Time: {:10.6f}s".format(train_time))
print("Accuracy: {:3.4f}%".format(acc))
##################################################################################

title = "Learning Curves KNN"
if use_best_params:
    if data_set is 'wine':
        clf = KNeighborsClassifier(n_neighbors=27)
    else:
        clf = KNeighborsClassifier(n_neighbors=82)
else:
    # Cross validation to get best estimators
    algorithm = KNeighborsClassifier()
    clf = GridSearchCV(algorithm, param_grids['knn'], cv=cv, iid=False, n_jobs=-1)

# Train Model
X_train, X_test, y_train, y_test = split_data(X, y, 0.2)

train_time = None
if use_best_params:
    start = timer()
    clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start
else:
    clf.fit(X_train, y_train)

if use_best_params:
    estimator = clf
else:
    estimator = clf.best_estimator_

# Plot Learning Curve
plot_learning_curve(estimator, title, X, y, cv=cv, n_jobs=-1)

# Final test on held out training data
y_pred = estimator.predict(X_test)
acc = accuracy_score(y_pred, y_test)

# Check Training Time If used cross validation
if not use_best_params:
    train_clf = KNeighborsClassifier(n_neighbors=clf.best_params_['n_neighbors'])
    start = timer()
    train_clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start

# Print Results
print("KNN:")
if not use_best_params:
    print(clf.best_params_.items())  # shows best params selected
print("Train Time: {:10.6f}s".format(train_time))
print("Accuracy: {:3.4f}%".format(acc))
# ##################################################################################

# Scale data for use in SVMs
min_max_scaler = MinMaxScaler()
X_minmax = min_max_scaler.fit_transform(X)

title = "Learning Curves Linear SVM"
if use_best_params:
    if data_set is 'wine':
        clf = SVC(C=1, cache_size=1000, kernel='linear')
    else:
        clf = SVC(C=100, cache_size=1000, kernel='linear', class_weight='balanced')
else:
    # Cross validation to get best estimators
    algorithm = SVC(cache_size=1000)
    clf = GridSearchCV(algorithm, param_grids['linear-svm'], cv=cv_svm, iid=False, n_jobs=-1)

# Train Model
X_train, X_test, y_train, y_test = split_data(X_minmax, y, 0.2)

train_time = None
if use_best_params:
    start = timer()
    clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start
else:
    clf.fit(X_train, y_train)

if use_best_params:
    estimator = clf
else:
    estimator = clf.best_estimator_

# Plot Learning Curve
plot_learning_curve(estimator, title, X_minmax, y, cv=cv_svm, n_jobs=-1)

# Final test on held out training data
y_pred = estimator.predict(X_test)
acc = accuracy_score(y_pred, y_test)

# Check Training Time If used cross validation
if not use_best_params:
    train_clf = SVC(C=clf.best_params_['C'], kernel='linear', cache_size=1000)
    start = timer()
    train_clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start

# Print Results
print("Linear SVM:")
if not use_best_params:
    print(clf.best_params_.items())  # shows best params selected
print("Train Time: {:10.6f}s".format(train_time))
print("Accuracy: {:3.4f}%".format(acc))
# ##################################################################################

title = "Learning Curves RBF SVM"
if use_best_params:
    if data_set is 'wine':
        clf = SVC(C=100, cache_size=1000, kernel='rbf', gamma='auto')
    else:
        clf = SVC(C=100, cache_size=1000, kernel='rbf', gamma='auto', class_weight='balanced')
else:
    # Cross validation to get best estimators
    algorithm = SVC(cache_size=1000)
    clf = GridSearchCV(algorithm, param_grids['rbf-svm'], cv=cv_svm, iid=False, n_jobs=-1)

# Train Model
X_train, X_test, y_train, y_test = split_data(X_minmax, y, 0.2)

train_time = None
if use_best_params:
    start = timer()
    clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start
else:
    clf.fit(X_train, y_train)

if use_best_params:
    estimator = clf
else:
    estimator = clf.best_estimator_

# Plot Learning Curve
plot_learning_curve(estimator, title, X_minmax, y, cv=cv_svm, n_jobs=-1)

# Final test on held out training data
y_pred = estimator.predict(X_test)
acc = accuracy_score(y_pred, y_test)

# Check Training Time If used cross validation
if not use_best_params:
    train_clf = SVC(C=clf.best_params_['C'], kernel='rbf', cache_size=1000, gamma='auto')
    start = timer()
    train_clf.fit(X_train, y_train)
    end = timer()
    train_time = end - start

# Print Results
print("RBF SVM:")
if not use_best_params:
    print(clf.best_params_.items())  # shows best params selected
print("Train Time: {:10.6f}s".format(train_time))
print("Accuracy: {:3.4f}%".format(acc))

plt.show()