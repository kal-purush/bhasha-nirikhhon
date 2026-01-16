import re
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib  # Import joblib

filtered_syslog_file_path = './data/Syslog/syslog.cvs'
model_filename = './data/Syslog/random_forest_model.joblib'
vectorizer_filename = './data/vectorizer.joblib'  # Choose a filename


# Preprocess log lines (remove timestamps and other noise)

df = pd.read_csv(filtered_syslog_file_path)
details = df["Detail"]
# Labels indicating whether a particular event of interest occurred (1 for occurrence, 0 for non-occurrence)
labels = df["Label"]

# Create a bag-of-words (BoW) vectorizer
vectorizer = CountVectorizer()

# Convert preprocessed log lines to feature vectors
feature_vectors = vectorizer.fit_transform(details)

# Split data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(
    feature_vectors, labels, test_size=0.2, random_state=42)

# Train a machine learning model (Random Forest classifier in this example)
clf = RandomForestClassifier(
    # n_estimators=100,  # Increase the number of trees
    max_depth=10,    # Allow trees to grow until fully developed
    # min_samples_split=2,  # Reduce the minimum samples required to split
    # min_samples_leaf=1,   # Allow smaller leaf nodes
    # max_features='sqrt',  # Consider all features for splitting
    class_weight='balanced',  # Adjust class weights for imbalanced data
    # bootstrap=True,   # Use bootstrapped samples
    random_state=42   # Set a specific random seed for reproducibility
)
clf.fit(X_train, y_train)

# Make predictions on the test set
y_pred = clf.predict(X_test)

# Evaluate the model
accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy:.2f}")

# Save the trained model to a file

joblib.dump(clf, model_filename)
joblib.dump(vectorizer, vectorizer_filename)

# In this example:

# The preprocess_log_line function removes timestamps and other noise from each log line using regular expressions.

# The CountVectorizer from scikit-learn is used to create a bag-of-words (BoW) representation of the preprocessed log lines.
# Each log line is converted into a feature vector.

# The data is split into training and testing sets.

# A machine learning model (Random Forest classifier) is trained on the training data to predict whether a specific event occurred based
# on the log lines.

# The model is evaluated using accuracy on the testing set.

# This is a simplified example, and in practice, you may need to perform more sophisticated
# feature engineering and experiment with different machine learning models to capture the patterns and relationships in your log data effectively. Additionally, you can extend this approach to handle more complex log line structures or incorporate additional features and techniques as needed.


# In the provided example, we are predicting whether a specific event or condition occurred based on the content of the log lines.
# To be more precise:

# The labels list contains binary labels for each log line:
# 1 indicates that a specific event or condition of interest occurred in that log line.
# 0 indicates that the event or condition did not occur in that log line.
# For instance, in the context of system log analysis, the events or conditions of interest could be various system states, errors, warnings,
# or other significant events. These events or conditions are typically defined based on the log data and the specific goals of your analysis.

# The goal of the machine learning model is to learn patterns in the log data that are indicative of the occurrence
# (or non-occurrence) of these events or conditions. Once trained, the model can predict whether an event or condition
# is likely to occur in new, unseen log lines.

# In practice, you would replace the placeholder comments like # Add more log lines here with actual log lines and adjust
# the labels list to reflect the presence or absence of the events or conditions you want to predict based on your log data.