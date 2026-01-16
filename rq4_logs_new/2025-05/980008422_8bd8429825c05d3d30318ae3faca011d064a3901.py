# -*- coding: utf-8 -*-
"""Improved Heart Disease Prediction"""

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler

# Data Collection and Processing
heart_data = pd.read_csv('data.csv', delimiter=',')
heart_data.columns = [f'feature_{i}' for i in range(len(heart_data.columns)-1)] + ['target']

# Check the data
print(heart_data.head())
print("\nTarget distribution:")
print(heart_data['target'].value_counts())

# Splitting the Features and Target
X = heart_data.drop(columns='target', axis=1)
y = heart_data['target'].map({'R': 0, 'M': 1})  # Convert to binary (0=Healthy, 1=Defective)

# Splitting the Data into Training & Test Data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# Feature Scaling
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Model Training - Logistic Regression
model = LogisticRegression(max_iter=1000)
model.fit(X_train, y_train)

# Model Evaluation
# Training data evaluation
y_train_pred = model.predict(X_train)
print("\nTraining Accuracy:", accuracy_score(y_train, y_train_pred))

# Test data evaluation
y_test_pred = model.predict(X_test)
print("Test Accuracy:", accuracy_score(y_test, y_test_pred))

# Detailed classification report
print("\nClassification Report:")
print(classification_report(y_test, y_test_pred))

# Confusion matrix
print("\nConfusion Matrix:")
print(confusion_matrix(y_test, y_test_pred))

# Building a Predictive System
def predict_heart_disease(input_data):
    if isinstance(input_data, (list, np.ndarray)):
        input_data = pd.DataFrame([input_data], columns=X.columns)
    
    scaled_input = scaler.transform(input_data)
    prediction = model.predict(scaled_input)
    probability = model.predict_proba(scaled_input)
    
    if prediction[0] == 0:
        return 'The person does not have heart disease (Probability: {:.2f}%)'.format(probability[0][0]*100)
    else:
        return 'The person has heart disease (Probability: {:.2f}%)'.format(probability[0][1]*100)


# Example prediction using first row from dataset
sample_input = X.iloc[[6]]
print("\nSample Prediction:")
print(predict_heart_disease(sample_input))


# Predict for the entire dataset
X_scaled = scaler.transform(X)  # Scale the whole dataset
predictions = model.predict(X_scaled)  # Get predictions
probabilities = model.predict_proba(X_scaled)  # Get probabilities

# Add predictions to original dataframe
results = heart_data.copy()
results['predicted'] = predictions
results['prob_0'] = probabilities[:, 0]  # Probability of class 0
results['prob_1'] = probabilities[:, 1]  # Probability of class 1

# View first few results
print(results.head(10))
