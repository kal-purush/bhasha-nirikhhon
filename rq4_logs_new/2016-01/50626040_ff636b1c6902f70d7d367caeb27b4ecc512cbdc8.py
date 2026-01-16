from sklearn.ensemble import RandomForestClassifier as rfc
from sklearn.datasets import load_iris

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


iris = load_iris()
df = pd.DataFrame(iris.data, columns = iris.feature_names)
df['is_train'] = np.random.uniform(0, 1, len(df)) <= .75
df['species'] = pd.Categorical.from_codes(iris.target, iris.target_names)

train, test = df[df['is_train'] == True], df[df['is_train'] == False]
features = df.columns[0:4]

forest = rfc(n_jobs = 2, n_estimators = 50)
y, _ = pd.factorize(train['species'])
forest.fit(train[features], y)

preds = iris.target_names[forest.predict(test[features])]
print pd.crosstab(index = test['species'], columns = preds, rownames = ['actual'], colnames = ['preds'])

importances = forest.feature_importances_
indices = np.argsort(importances)

plt.figure(1)
plt.title('Feature importances')
plt.barh(range(len(indices)), importances[indices], color = 'b', align = 'center')
plt.yticks(range(len(indices)), features[indices])
plt.xlabel('Relative importance')
plt.show()
raw_input()