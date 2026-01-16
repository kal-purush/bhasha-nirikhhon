"""
Tune an XGBoost model on a heart disease dataset
by ignoring a few columns of data.
"""

from heapq import nlargest
import logging as log
from numpy import mean
import pandas
from sklearn.model_selection import (
  StratifiedKFold,
  cross_val_score,
)
from sklearn.preprocessing import LabelEncoder
from sys import argv, executable
from tqdm import tqdm
from typing import Iterable
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
original_data = pandas.read_csv(data_path)
log.info(f'Loaded dataset {data_path} to memory')

# transform values to numeric
for col in original_data.columns:
  if original_data[col].dtype == 'object':
    original_data[col] = LabelEncoder().fit_transform(original_data[col])
log.info('Transformed values')

def powerset(iterable: Iterable, max_length: int):
  from itertools import chain, combinations
  s = list(iterable)
  return chain.from_iterable(
    combinations(s, r)
    for r in range(min(max_length, len(s)) + 1)
  )

# define model
model = XGBClassifier(
  n_jobs=2,
  random_state=6,
  n_estimators=3,
  learning_rate=0.01,
  max_depth=8,
  min_child_weight=42,
  scale_pos_weight=5,
  subsample=0.6,
  reg_alpha=0,
  reg_lambda=3.75,
  gamma=0.5,
)
log.debug(f'model:\n{model}')

irrelevant_feature_sets: list[tuple[float, str]] = []

for irrelevant_features in tqdm(list(powerset(original_data.columns, 2))):
  data = original_data.copy(deep=True)
  target = 'HadHeartAttack'

  # remove irrelevant features
  irrelevant_features = list(irrelevant_features)
  if target in irrelevant_features: # target is always relevant
    continue
  data.drop(columns=irrelevant_features, inplace=True)
  log.debug(f'Removed irrelevant features: {irrelevant_features}')

  # remove rows with missing values
  data.dropna(inplace=True)
  log.debug('Removed rows with missing values')

  # define target and features
  x = data.drop(columns=target)
  y = data[target]

  # evaluate model
  log.debug('Started model evaluation')

  # cross validation
  scores = cross_val_score(
    model, x, y,
    scoring='roc_auc',
    cv=StratifiedKFold(n_splits=3, shuffle=True, random_state=6),
    n_jobs=4,
  )
  average_auc = mean(scores)
  log.debug('Terminated model evaluation')
  log.debug(f'AUCs:\n{scores}')
  log.debug(f'Average AUC: {average_auc}')

  # update irrelevant feature sets
  irrelevant_feature_sets.append((average_auc, irrelevant_features))

# show top 10 irrelevant feature sets in order of AUC score
top_irrelevant_feature_sets = nlargest(10, irrelevant_feature_sets)
log.info(f'Top 10 irrelevant feature sets:')
for score, features in top_irrelevant_feature_sets:
  log.info(f'{features}: {score}')