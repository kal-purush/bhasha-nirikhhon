import pandas
import numpy
#from scipy.stats import mode
from sklearn import neighbors
from sklearn.neighbors import DistanceMetric
from pprint import pprint

TITANIC_TRAINING_DATA = '/Users/danielzwelling/Documents/coding_stuff/code_fellows/python-401/Pod1/train.csv'
TITANIC_TESTING_DATA = '/Users/danielzwelling/Documents/coding_stuff/code_fellows/python-401/Pod1/test.csv'
titanic_dataframe = pandas.read_csv(TITANIC_TRAINING_DATA, header=0)
bigness_of_data = len(titanic_dataframe)
print(bigness_of_data)