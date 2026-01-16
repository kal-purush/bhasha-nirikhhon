# --------------------------------------------------------------------------------------------------
# Technical interview
# By Joost van der Linden, February 11th, 2019
#
# File contains: abstracted machine learning code
# 
# --------------------------------------------------------------------------------------------------
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score, roc_auc_score

def analyze_model(df_train, df_test, features, target, estimator):
	''' Performs a simple 80/20 split and reports performance on training and test dataset.

	Args:
		df_train (:obj:`pandas.DataFrame`): Dataframe containing features and target variable.
		df_test (:obj:`pandas.DataFrame`): Dataframe containing features and target variable.
		features (:obj:`list`): List of features to use.
		target (:obj:`str`): Target variable.
		estimator (:obj:`sklearn.base.BaseEstimator): scikit-learn model to use.

	Returns:
		estimator (:obj:`sklearn.base.BaseEstimator): fitted scikit-learn model.
	'''

	# Training data
	X      = df_train[features]
	y      = df_train[target]
	X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=41)   

	# Testing data
	X_test = df_test[features]
	y_test = df_test[target]
	
	# Performance on the validation data
	print('--- Validation set performance ---')
	estimator.fit(X_train, y_train)
	model_performance(estimator, X_val, y_val)

	# Performance on the test data
	print('--- Test set performance ---')
	estimator.fit(X, y) # overwrites previous fit
	model_performance(estimator, X_test, y_test)

	return estimator

def model_performance(estimator, X, y):
	''' Reports model performance in terms of (baseline) accuracy, AUC and precision-recall.
	
	Args:
		estimator (:obj:`sklearn.base.BaseEstimator): fitted scikit-learn model.
		X (:obj:`pandas.DataFrame`): Data to predict on.
		y (:obj:`pandas.Series`): Truth.
	'''

	y_pred      = estimator.predict(X)

	#predictions = [round(value) for value in y_pred]
	accuracy    = accuracy_score(y, y_pred)
	auc         = roc_auc_score(y, y_pred)

	print("Baseline accuracy: \n\n{}\n".format(y.value_counts(normalize = True)*100))
	print("Model accuracy:    {:.2f}".format(accuracy * 100))
	print("AUC:               {:.2f}".format(auc))
	print()
	print(classification_report(y, y_pred))
