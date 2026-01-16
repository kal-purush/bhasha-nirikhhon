from sklearn.model_selection import GridSearchCV 
from sklearn.linear_model import LogisticRegression 
from sklearn.pipeline import Pipeline

clf = Pipeline([
  ("kpca", KernelPCA(n_components=2)),
  ("log_reg", LogisticRegression())
])

param_grid = [{
  "kpca__gamma": np.linspace(0.03, 0.05, 10),
  "kpca__kernel": ["rbf", "sigmoid"]
}]

grid_search = GridSearchCV(clf, param_grid, cv=3)
grid_search.fit(X, y)