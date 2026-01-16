import matplotlib.pyplot as plt
import numpy as np

from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
from sklearn.metrics import accuracy_score
from sklearn.metrics import log_loss
from sklearn.model_selection import StratifiedShuffleSplit
from sktime.classification.dictionary_based import BOSSEnsemble

from sktime.datasets import load_UCR_UEA_dataset

from models.utils.pysize import get_size

if __name__ == "__main__":
    print(f'Run {__file__}')
    # BasicMotions, ArrowHead, Haptics, SonyAIBORobotSurface2

    dataset_name = 'SonyAIBORobotSurface1'
    X, y = load_UCR_UEA_dataset(name=dataset_name, return_X_y=True)
    # X_test, y_test = load_UCR_UEA_dataset(name=dataset_name, split='train', return_X_y=True)
    # print(X_train.shape, y_train.shape, X_test.shape, y_test.shape)

    # dataset_name = 'KR2700'
    # train_load_path = Path(
    #     'data_for_tests/RobotMBConsumption/RobotMBConsumption_TRAIN.ts')
    # test_load_path = Path(
    #     'data_for_tests/RobotMBConsumption/RobotMBConsumption_TEST.ts')
    # X_train, y_train = load_from_tsfile_to_dataframe(train_load_path)
    # X_test, y_test = load_from_tsfile_to_dataframe(test_load_path)
    # print(X_train.shape, y_train.shape, X_test.shape, y_test.shape)

    # dataset_name = 'KR2700'
    # data_load_path = Path('data_for_tests/RobotMBConsumption/RobotMBConsumption_ALL.ts')

    # dataset_name = 'KR5'
    # data_load_path = Path(r'N:\Datasets\TimeSeries\classification\IndustrialRobots\RobotKR5Consumption\RobotKR5Consumption_TRAIN.ts')
    # # data_load_path = Path(r'D:\DATA_FAST\RobotKR5Consumption\RobotKR5Consumption_TRAIN.ts')
    
    # X, y = load_from_tsfile_to_dataframe(data_load_path)

    labels, counts = np.unique(y, return_counts=True)
    print('Complete dataset labels and counts:')
    print(labels, counts)
    stratified_splitter = StratifiedShuffleSplit(n_splits=1, train_size=0.01, random_state=42)
    for tr_idx, test_idx in stratified_splitter.split(X, y):
        X_train, X_test, y_train, y_test = X.iloc[tr_idx, [0]], X.iloc[test_idx, [0]], y.iloc[tr_idx], y.iloc[test_idx]
        print(X_train.shape, y_train.shape, X_test.shape, y_test.shape)
        labels, counts = np.unique(y_train, return_counts=True)
        print(labels, counts)

    cf = BOSSEnsemble(n_jobs=4)
    cf.fit(X_train, y_train)
    score_boss = cf.score(X_test, y_test)
    print(f'BOSS score: {score_boss}')
    y_pred = cf.predict(X_test)
    y_pred_proba = cf.predict_proba(X_test)
    xentropy = log_loss(y_test, y_pred_proba)
    print(f'Crossentropy: {xentropy:.2f}')

    labels_list = cf.classes_
    cm_raw = confusion_matrix(
        y_test, y_pred,
        labels=labels_list
    )
    cm_normalized = confusion_matrix(
        y_test, y_pred,
        labels=labels_list,
        normalize='true'
    )

    fig, axes = plt.subplots(2, figsize=(6, 8))

    cm_raw_display = ConfusionMatrixDisplay(
        cm_raw,
        display_labels=labels_list
    ).plot(ax=axes[0], cmap=plt.cm.Blues)
    cm_norm_display = ConfusionMatrixDisplay(
        cm_normalized,
        display_labels=labels_list
    ).plot(ax=axes[1], cmap=plt.cm.Blues)
    plt.tight_layout()
    print(f'Accuracy {accuracy_score(y_test, y_pred)}')
    print(f'Size of classifier: {get_size(cf)} bytes.')
    print(f'Size of train dataset: {get_size(X_train)} bytes.')
    plt.show()

    print('End of tests.')

else:
    print(f'Importing {__file__}')