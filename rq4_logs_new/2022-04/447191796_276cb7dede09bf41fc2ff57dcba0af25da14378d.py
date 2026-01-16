from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
import pickle

class LRModel(object):

    def __init__(self, max_iter=1000):
        self.clf = LogisticRegression(max_iter=max_iter)
        self.scaler = StandardScaler()

    def scaler_fit(self, x):
        '''
        Fits the scaler
        '''
        self.scaler.fit(x)

    def scale_input(self, x):
        return self.scaler.transform(x)

    def pickle_model(self, path='pickles/logistic_regressor.pkl'):
        with open(path, 'wb') as f:
            pickle.dump(self.clf, f)
            print("Pickled classifier at {}".format(path))

    def pickle_scaler(self, path='pickles/standard_scaler.pkl'):
        with open(path, 'wb') as f:
            pickle.dump(self.scaler, f)
            print("Pickled scaler at {}".format(path))

    def load_model(self, path="pickles/logistic_regressor.pkl"):
        with open(path, 'rb') as f:
            self.clf = pickle.load(f)

    def load_scaler(self, path="pickles/standard_scaler.pkl"):
        with open(path, 'rb') as f:
            self.scaler = pickle.load(f)

    def initialize_model(self):
        self.load_model()
        self.load_scaler()

    def predict(self, x):
        return self.clf.predict(x)