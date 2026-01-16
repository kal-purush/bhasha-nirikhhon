import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn import preprocessing
from collections import Counter

#read subsidy
train_subsidy = pd.read_csv('../data/train/subsidy_train.txt', header = None)
train_subsidy.columns = ['ID', 'MONEY']
test_subsidy = pd.read_csv('../data/test/studentID_test.txt', header = None)
test_subsidy.columns = ['ID']
test_subsidy['MONEY'] = np.nan
train_test = pd.concat([train_subsidy, test_subsidy])
train_test.to_csv("../data/input/train_test.csv", index = False)

#read card
train_card = pd.read_csv('../data/train/card_train.txt', header = None)
train_card.columns = ['ID', 'CARD_CAT', 'CARD_WHERE', 'CARD_HOW', 'CARD_TIME', 'CARD_SPEND', 'CARD_REMAINDER']
test_card = pd.read_csv('../data/test/card_test.txt', header = None)
test_card.columns = ['ID', 'CARD_CAT', 'CARD_WHERE', 'CARD_HOW', 'CARD_TIME', 'CARD_SPEND', 'CARD_REMAINDER']
card_train_test = pd.concat([train_card,test_card])


#process card data
card = pd.DataFrame(card_train_test.groupby(['ID'])['CARD_CAT'].count())

card['CARD_SPEND_SUM'] = card_train_test.groupby(['ID'])['CARD_SPEND'].sum()
card['CARD_SPEND_MEAN'] = card_train_test.groupby(['ID'])['CARD_SPEND'].mean()
card['CARD_SPEND_STD'] = card_train_test.groupby(['ID'])['CARD_SPEND'].max()
card['CARD_SPEND_MEDIAN'] = card_train_test.groupby(['ID'])['CARD_SPEND'].median()

card['CARD_REMAINDER_SUM'] = card_train_test.groupby(['ID'])['CARD_REMAINDER'].sum()
card['CARD_REMAINDER_MEAN'] = card_train_test.groupby(['ID'])['CARD_REMAINDER'].mean()
card['CARD_REMAINDER_STD'] = card_train_test.groupby(['ID'])['CARD_REMAINDER'].max()
card['CARD_REMAINDER_MEDIAN'] = card_train_test.groupby(['ID'])['CARD_REMAINDER'].median()

card.to_csv('../data/input/cardInfo.csv', index = True)
card = pd.read_csv('../data/input/cardInfo.csv')


#read score
train_score = pd.read_csv('../data/train/score_train.txt', header = None)
train_score.columns = ['ID', 'COLLEGE', 'RANK']
test_score = pd.read_csv('../data/test/score_test.txt', header = None)
test_score.columns = ['ID', 'COLLEGE', 'RANK']
train_test_score = pd.concat([train_score, test_score])

score = pd.DataFrame(train_test_score.groupby(['COLLEGE'])['RANK'].max())
score.to_csv('../data/input/collegeInfo.csv', index = True)
score = pd.read_csv('../data/input/collegeInfo.csv')
score.columns = ['COLLEGE', 'COLLEGE_STU_NUM']

train_test_score = pd.merge(train_test_score, score, how='left', on='COLLEGE')
train_test_score['SCORE'] = train_test_score['RANK'] / train_test_score['COLLEGE_STU_NUM']
train_test_score.to_csv("../data/input/train_test_score.csv", index = False)
