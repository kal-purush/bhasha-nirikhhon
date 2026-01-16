from sklearn.datasets import make_classification
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import RandomizedSearchCV
from scipy.stats import uniform


# Applying RandomSearchCV() for a single perceptron AND gate
print(" finding AND gate using the suitable Hyperparameters and applying RandomSearchCV()")

# Generate a random classification dataset
X, y = make_classification(n_samples=3000, n_features=10, random_state=42)

# Define the model and hyperparameter distributions
model = LogisticRegression(random_state=42)
param_dist = {'C': uniform(loc=0, scale=4),
              'penalty': ['l1', 'l2']}

# Create the RandomizedSearchCV object
random_search = RandomizedSearchCV(model, param_distributions=param_dist,
                                   n_iter=100, cv=5, random_state=42,
                                   n_jobs=-1, verbose=1)

# Fit the RandomizedSearchCV object to the data
random_search.fit(X, y)

# Print the best hyperparameters and score
print(f"Best hyperparameters: {random_search.best_params_}")
print(f"Best score: {random_search.best_score_:.4f}")
