"""
Tune an XGBoost model on a heart disease dataset
using GridSearchCV with stratified k fold method.
"""

import logging as log
from numpy import linspace, logspace
import pandas
from sklearn.model_selection import (
  GridSearchCV,
  StratifiedKFold,
)
from sklearn.preprocessing import LabelEncoder
from sys import argv, executable
from xgboost import XGBClassifier

log.basicConfig(
  level=log.INFO,
  format='%(levelname)s\t%(asctime)s.%(msecs)03d\t%(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
)

pandas.options.mode.chained_assignment = None

# check arguments
argc = len(argv)
if argc != 2:
  raise TypeError(f'\
Program expected exact 2 arguments, got {argc}. \
Usage: {executable} {argv[0]} <data_path>')

# get arguments
data_path = argv[1]

# read data
data = pandas.read_csv(data_path)
log.info(f'Loaded dataset {data_path} to memory')

# remove irrelevant features
irrevalent_features = [
  'BMI',
  'DifficultyDressingBathing',
  'HighRiskLastYear',
]
data.drop(columns=irrevalent_features, inplace=True)
log.info('Removed irrelevant features')

# remove rows with missing values
data.dropna(inplace=True)
log.info('Removed rows with missing values')

# define target and features
target = 'HadHeartAttack'
x = data.drop(columns=target)
y = data[target]

# transform values to numeric
for col in x.columns:
  if x[col].dtype == 'object':
    x[col] = LabelEncoder().fit_transform(x[col])
y = pandas.Series(LabelEncoder().fit_transform(y))
log.info('Transformed values')

# define model
model = GridSearchCV(
  estimator=XGBClassifier(
    random_state=6,
    n_jobs=4,
    n_estimators=1,
    num_parallel_tree=1,
    learning_rate=0.01,
  ),
  param_grid={
    'max_depth': [8],
    'min_child_weight': [42],
    'scale_pos_weight': [5],
    'subsample': [0.6],
    'reg_alpha': [0],
    'reg_lambda': [3.75],
    'gamma': [0.5],
  },
  scoring='roc_auc',
  cv=StratifiedKFold(n_splits=10, shuffle=True, random_state=6),
  n_jobs=1,
  verbose=1,
)

# tuning parameters
log.info('Started model parameter tuning')

# grid search cross validation
result = model.fit(x, y)
log.info('Terminated model parameter tuning')
log.info(f'Best AUC score: {result.best_score_}')
log.info(f'Best parameters: {result.best_params_}')