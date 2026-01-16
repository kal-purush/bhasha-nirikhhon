from sklearn.decomposition import TruncatedSVD
from sklearn.datasets import load_iris
import matplotlib.pyplot as plt
%matplotlib inline

iris_dataset = load_iris()
X = iris_dataset.data
Y = iris_dataset.target

# 비교
## SVD 적용 전
from sklearn.model_selection import train_test_split
train_x, test_x, train_y, test_y = train_test_split(X,           
                                                    Y,            
                                                    train_size=0.7, 
                                                    random_state=2021)

from sklearn.neighbors import KNeighborsClassifier as knn_c 

m_knn = knn_c(5)
m_knn.fit(train_x, train_y)
m_knn.score(train_x, train_y) * 100   # 97.14
m_knn.score(test_x, test_y) * 100   # 97.78

## SVD 적용 후
svd = TruncatedSVD(n_components = 2)
svd.fit(X)
iris_svd = svd.transform(X)

svd_train_x, svd_test_x, train_y, test_y = train_test_split(iris_svd,           
                                                            Y,              
                                                            train_size=0.7, 
                                                            random_state=2021)

m_knn = knn_c(5)
m_knn.fit(svd_train_x, train_y)
m_knn.score(svd_train_x, train_y) * 100   # 97.14
m_knn.score(svd_test_x, test_y) * 100   # 97.78

# 시각화
plt.scatter(x = iris_svd[:, 0], y= iris_svd[:, 1], c = Y)
plt.xlabel('Component 1')
plt.ylabel('Component 2')
