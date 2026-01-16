import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline, make_union
from tpot.builtins import StackingEstimator


class Config:
    def __init__(self):
        self.trainingdata_path = "./data/trainingdata_bool.npy"
        self.testingdata_path = "./data/test_x.npy"


DGBDT_config = Config()

if __name__ == '__main__':
    trainingdata = np.load(DGBDT_config.trainingdata_path)
    x = trainingdata[:, 0:-1]
    y = trainingdata[:, -1].reshape(-1, 1)
    train_x, validation_x, train_y, validation_y = train_test_split(x, y, test_size=0.368, random_state=None)

    pipeline = make_pipeline(
    StackingEstimator(estimator=GradientBoostingClassifier(learning_rate=0.001, max_depth=9, max_features=0.8500000000000001, min_samples_leaf=12, min_samples_split=5, n_estimators=100, subsample=0.6000000000000001)),
    GradientBoostingClassifier(learning_rate=0.01, max_depth=7, max_features=0.6000000000000001, min_samples_leaf=6, min_samples_split=10, n_estimators=100, subsample=0.6500000000000001)
)

    pipeline.fit(train_x, train_y)
    # prediction_train = pipeline.predict(train_x)
    # prediction_val = pipeline.predict(validation_x)
    # mse = [np.mean(np.square(prediction_train - train_y)) / 2,
    #        np.mean(np.square(prediction_val - validation_y)) / 2]
    # print(mse)
    score_train = pipeline.score(train_x, train_y)
    score_test = pipeline.score(validation_x, validation_y)
    print(score_train, score_test)

    testingdata = np.load(DGBDT_config.testingdata_path)
    predict = pipeline.predict(testingdata)
    print(predict)
    np.savetxt('./data/predict/predict.txt', predict)
