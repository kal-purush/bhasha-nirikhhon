"""
Surprise 패키지로 학습된 추천 알고리즘을 기반으로
특정 사용자가 아직 평점을 매기지 않은 (관람하지 않은) 영화 중에서
개인 취향에 가장 적절한 영화를 추천
"""
# Surprise를 활용한 개인화 추천
# 'n_epochs': 20, 'n_factors': 50

import pandas as pd
from surprise import Reader, Dataset
from surprise import accuracy
from surprise.model_selection import train_test_split
from surprise import SVD

# 데이터 분리 과정 없이 전체를 학습 데이터로 사용
# 오류 발생
ratings = pd.read_csv("../dataset/ml-latest-small/ratings.csv")

# reader = Reader(rating_scale = (0.5, 5))
# data = Dataset.load_from_file(ratings['userId', 'moviId', 'rating'], reader)
# algo = SVD(n_factor=50, random=0)
# algo.fit(data)



# build_full_trainset() 메서드를 호출
# 전체 데이터를 학습 데이터로 이용할 수 있다

from surprise.dataset import DatasetAutoFolds
reader = Reader(line_format='user item rating timestamp', sep=',', rating_scale=(0, 0.5))
data_folds = DatasetAutoFolds(ratings_file='../dataset/ml-latest-small/ratings_noh.csv', reader=reader)
trainset = data_folds.build_full_trainset()

# 학습
algo = SVD(n_epochs=20, n_factors=50, random_state=0)
algo.fit(trainset)

# userId =9 사용자로 지정하여 테스트 진행
movies = pd.read_csv(r"../dataset/ml-latest-small/movies.csv")
movieid = ratings[ratings['userId'] == 9]['movieId']
print(movieid)
print(movieid== 42)

# movieid==42
print(movieid['movieid' == 42].count())

# if movieid[movieid == 42].count() == 0:
#     print("There is no rating of 42 for user 9")
#
# # print(movies[movies['movieId']] == 42)
# print(movies['movieId'])


# uid = str(9)
# iid = str(42)
#
# pred = algo.predict(uid, iid, verbose=True)