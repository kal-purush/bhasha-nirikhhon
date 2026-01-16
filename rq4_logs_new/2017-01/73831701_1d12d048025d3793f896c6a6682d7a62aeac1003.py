from matplotlib import pyplot as plt
import itertools
import numpy as np


def plot_loss(models, legend, title):
    for i in range(len(models)):
        plt.plot(models[i].history.get('loss'))
    plt.title(title)
    plt.ylabel('loss')
    plt.xlabel('epoch')
    plt.legend(legend, loc='upper right')
    plt.show()


def plot_confusion_matrix(cm, classes,
                          normalize=False,
                          title='Confusion matrix',
                          cmap=plt.cm.Blues):
    """
    This function prints and plots the confusion matrix.
    Normalization can be applied by setting `normalize=True`.
    """
    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45)
    plt.yticks(tick_marks, classes)

    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, cm[i, j],
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")

    plt.tight_layout()
    plt.ylabel('True label')
    plt.xlabel('Predicted label')


def plot_confusion_matrix_performance(cnf_matrix, classes, model_name=""):
    # Plot non-normalized confusion matrix
    # plt.figure()
    # plot_confusion_matrix(cnf_matrix, classes=class_names,
    #                           title=model_name + ' Confusion matrix, without normalization')

    # Plot normalized confusion matrix
    plt.figure()
    plot_confusion_matrix(cnf_matrix, classes=classes, normalize=True,
                          title=model_name)
    plt.show()

    TP = cnf_matrix[0, 0]
    FN = cnf_matrix[0, 1]
    FP = cnf_matrix[1, 0]
    TN = cnf_matrix[1, 1]

    accuracy = (TN + TP) / (TP + FN + FP + TN)
    sensitivity = TP / (TP + FN)
    specificity = TN / (TN + FP)

    print("Accuracy: %f" % accuracy)
    print("Sensitivity: %f" % sensitivity)
    print("Specificity: %f" % specificity)