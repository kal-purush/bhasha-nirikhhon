import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

class CustomLinearRegression:
    """Custom linear regression"""

    def __init__(self, fit_intercept=True):
        self.fit_intercept = fit_intercept
        self.coefficient = None
        self.intercept = None

    def fit(self, X, y):
        """Fit"""

        if self.fit_intercept:
            X = np.column_stack((np.ones(len(X)), X))

        X_transpose = X.T
        self.coefficient = np.linalg.inv(X_transpose.dot(X)) @ X_transpose @ y

        if self.fit_intercept:
            self.intercept = self.coefficient[0]
            self.coefficient = self.coefficient[1:]
        else:
            self.intercept = 0

    def predict(self, X):
        """Predict"""

        if self.fit_intercept:
            X = np.column_stack((np.ones(len(X)), X))
        return X.dot(np.hstack(([self.intercept], self.coefficient)))

    def score(self, y, y_pred):
        """Score"""

        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r2 = 1 - ss_res / ss_tot
        return r2

    def rmse(self, y, y_pred):
        """rmse"""

        mse = np.mean((y - y_pred) ** 2)
        rmse = np.sqrt(mse)
        return rmse


x = np.array([4, 4.5, 5, 5.5, 6, 6.5, 7])
w = np.array([1, -3, 2, 5, 0, 3, 6])
z = np.array([11, 15, 12, 9, 18, 13, 16])
y = np.array([33, 42, 45, 51, 53, 61, 62])

X = np.column_stack((x, w, z))

model = CustomLinearRegression(fit_intercept=True)
model.fit(X, y)
print(f'Coefficient:{model.coefficient}')
y_pred = model.predict(X)
print(f"y:{y}\ny predict:{y_pred}\nScore:{model.score(y, y_pred)}\nRMSE:{model.rmse(y, y_pred)}")

sklearn_model = LinearRegression(fit_intercept=True)
sklearn_model.fit(X, y)
y_pred_sklearn = sklearn_model.predict(X)

score_sklearn = r2_score(y, y_pred_sklearn)
rmse_sklearn = np.sqrt(mean_squared_error(y, y_pred_sklearn))

print(f"Score (sklearn): {score_sklearn}")
print(f"RMSE (sklearn): {rmse_sklearn}")