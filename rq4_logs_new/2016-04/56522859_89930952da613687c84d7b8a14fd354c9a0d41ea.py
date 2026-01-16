clfr = MyClassifier()
clfr2 = MyClassifierTwo()


# In[317]:

get_ipython().magic(u'timeit clfr.fit(X, Y)')


# In[555]:

myfit = clfr.fit(X, Y)


# In[470]:

myfit = clfr2.fit(X, Y)


# In[512]:

print len(clfr.model)
print len(clfr2.model)


# In[513]:

len(clfr.used_features)


# In[556]:

backup = clfr.model


# ### based on combined result

# In[551]:

X_test = exp_all[testing_idx]
Yprime_test = clfr.predict(X_test)


# In[547]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# ### New

# In[471]:

X_test = exp1[testing_idx]
Yprime_test = clfr2.predict(X_test)


# In[472]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# In[473]:

res3 = []
backup2 = clfr2.model
for num_tree in range(1, len(backup2)):
    clfr2.model = backup2[0:num_tree]
    X_test = exp1[testing_idx]
    Yprime_test = clfr2.predict(X_test)
    res3.append((num_tree, sum(health_classes[testing_idx] == Yprime_test)/float(len(testing_idx))))


# In[475]:

len(clfr2.model)


# In[474]:

# This is necessary for using matplotlib in jupyter
get_ipython().magic(u'matplotlib inline')

# Import the necessary modules and libraries
import numpy as np
import matplotlib.pyplot as plt

X = [r[0] for r in res3]
y = [r[1] for r in res3]

# Plot the results
plt.figure()
plt.scatter(X, y, c="k", label="data")
plt.xlabel("data")
plt.ylabel("target")
plt.title("Decision Tree Accuracy")
plt.legend()
plt.show()


# ### Original Classifier's plot

# In[557]:

res = []
for num_tree in range(1, len(backup)):
    clfr.model = backup[0:num_tree]
    X_test = exp_all[testing_idx]
    Yprime_test = clfr.predict(X_test)
    res.append((num_tree, sum(health_classes[testing_idx] == Yprime_test)/float(len(testing_idx))))


# In[562]:

# This is necessary for using matplotlib in jupyter
get_ipython().magic(u'matplotlib inline')

# Import the necessary modules and libraries
import numpy as np
import matplotlib.pyplot as plt

Xpts = [r[0] for r in res]
y = [r[1] for r in res]

# Plot the results
plt.figure()
plt.scatter(Xpts, y, c="k", label="Accuracy")
plt.xlabel("Number of trees")
plt.ylabel("Accuracy")
plt.title("Decision Forests Accuracy")
plt.vlines(3, 0.1, 0.8)
plt.vlines(25, 0.1, 0.8)
plt.vlines(55, 0.1, 0.8)
plt.vlines(79, 0.1, 0.8)
plt.vlines(97, 0.1, 0.8)
plt.legend()
plt.show()


# #### I was about to say the later trees are almost as informative as the

# In[523]:

res2 = []
for num_tree in range(len(backup)-1, 0, -1):
    clfr.model = backup[num_tree:len(backup)]
    X_test = exp_all[testing_idx]
    Yprime_test = clfr.predict(X_test)
    res2.append((num_tree, sum(health_classes[testing_idx] == Yprime_test)/float(len(testing_idx))))


# In[524]:

# This is necessary for using matplotlib in jupyter
get_ipython().magic(u'matplotlib inline')

# Import the necessary modules and libraries
import numpy as np
import matplotlib.pyplot as plt

Xpts = [r[0] for r in res2]
y = [r[1] for r in res2]

# Plot the results
plt.figure()
plt.scatter(Xpts, y, c="k", label="data")
plt.xlabel("data")
plt.ylabel("target")
plt.title("Decision Tree Accuracy")
plt.legend()
plt.show()


# In[ ]:




# ### SVM, WTF?

# In[433]:

from sklearn import svm

svmclf = svm.SVC()
svmclf.fit(X, Y)


# In[434]:

X_test = exp1[testing_idx]
Yprime_test = svmclf.predict(X_test)


# In[435]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# In[ ]:




# In[ ]:




# In[ ]:




# ### Basic Tree
#

# In[526]:

from sklearn import tree
X = exp_all[training_idx]
Y = health_classes[training_idx]
clf = tree.DecisionTreeClassifier()
clf = clf.fit(X, Y)


# In[527]:

X_test = exp_all[testing_idx]
Yprime_test = clf.predict(X_test)


# In[528]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# ## AdaBoost

# In[529]:

X = exp_all[training_idx]
Y = health_classes[training_idx]


# In[530]:

from sklearn.ensemble import AdaBoostClassifier

clf = AdaBoostClassifier()
clf = clf.fit(X, Y)
X_test = exp_all[testing_idx]
Yprime_test = clf.predict(X_test)


# In[531]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# In[167]:

from sklearn.ensemble import AdaBoostClassifier

clf = AdaBoostClassifier(n_estimators=20)
clf = clf.fit(X, Y)
X_test = exp1[testing_idx]
Yprime_test = clf.predict(X_test)


# #### No idea what 7 these are

# In[242]:

from sklearn.ensemble import AdaBoostClassifier

clf = AdaBoostClassifier(n_estimators=70, learning_rate=1.0)
clf = clf.fit(X, Y)
X_test = exp1[testing_idx]
Yprime_test = clf.predict(X_test)


# In[528]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# In[180]:

clf.decision_function(X_test[0:10])


# In[213]:

import operator
imp = zip(range(0, 22283), clf.feature_importances_)
imp.sort(key=operator.itemgetter(1), reverse=True)
imp[0:20]


# In[182]:

health_classes[testing_idx][0:10]


# In[168]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# #### Is AdaBoost deterministic?

# ## Bagging

# In[532]:

from sklearn.ensemble import BaggingClassifier
clf = BaggingClassifier()
clf = clf.fit(X, Y)
X_test = exp_all[testing_idx]
Yprime_test = clf.predict(X_test)


# In[533]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# #### Bagging with more estimators

# In[105]:

from sklearn.ensemble import BaggingClassifier
clf = BaggingClassifier(n_estimators=50)
clf = clf.fit(X, Y)
X_test = exp1[testing_idx]
Yprime_test = clf.predict(X_test)


# In[106]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# ##### Another run may give very different results

# In[108]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# #### Bagging with even more estimators

# In[130]:

from sklearn.ensemble import BaggingClassifier
clf = BaggingClassifier(n_estimators=150)
clf = clf.fit(X, Y)
X_test = exp1[testing_idx]
Yprime_test = clf.predict(X_test)


# In[131]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# ### Gradient Boosting

# In[534]:

from sklearn.ensemble import GradientBoostingClassifier
clf = GradientBoostingClassifier()
clf = clf.fit(X, Y)
X_test = exp_all[testing_idx]
Yprime_test = clf.predict(X_test)


# In[535]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# In[121]:

from sklearn.ensemble import GradientBoostingClassifier
clf = GradientBoostingClassifier(max_depth=5)
clf = clf.fit(X, Y)
X_test = exp1[testing_idx]
Yprime_test = clf.predict(X_test)


# In[123]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# In[ ]:




# ### RandomForestClassifier

# In[536]:

from sklearn.ensemble import RandomForestClassifier
clf = RandomForestClassifier()
clf = clf.fit(X, Y)
X_test = exp_all[testing_idx]
Yprime_test = clf.predict(X_test)


# In[537]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# In[127]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# #### adding more trees do not help much

# In[128]:

from sklearn.ensemble import RandomForestClassifier
clf = RandomForestClassifier(n_estimators=150)
clf = clf.fit(X, Y)
X_test = exp1[testing_idx]
Yprime_test = clf.predict(X_test)


# In[129]:

print collections.Counter(health_classes[testing_idx] == Yprime_test)
print collections.Counter(zip(health_classes[testing_idx], health_classes[testing_idx] == Yprime_test))


# In[15]:

# This is necessary for using matplotlib in jupyter
get_ipython().magic(u'matplotlib inline')

# Import the necessary modules and libraries
import numpy as np
from sklearn.tree import DecisionTreeRegressor
import matplotlib.pyplot as plt

# Create a random dataset
rng = np.random.RandomState(1)
X = np.sort(5 * rng.rand(80, 1), axis=0)
y = np.sin(X).ravel()
y[::5] += 3 * (0.5 - rng.rand(16))

# Fit regression model
regr_1 = DecisionTreeRegressor(max_depth=2)
regr_2 = DecisionTreeRegressor(max_depth=5)
regr_1.fit(X, y)
regr_2.fit(X, y)

# Predict
X_test = np.arange(0.0, 5.0, 0.01)[:, np.newaxis]
y_1 = regr_1.predict(X_test)
y_2 = regr_2.predict(X_test)

# Plot the results
plt.figure()
plt.scatter(X, y, c="k", label="data")
plt.plot(X_test, y_1, c="g", label="max_depth=2", linewidth=2)
plt.plot(X_test, y_2, c="r", label="max_depth=5", linewidth=2)
plt.xlabel("data")
plt.ylabel("target")
plt.title("Decision Tree Regression")
plt.legend()
plt.show()