# data_prep.py
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from nltk.corpus import stopwords
import re
import nltk

# Ensure you have NLTK stopwords downloaded
nltk.download('stopwords')

# Load the dataset
data = pd.read_csv('../data/spam_ham_dataset.csv')

# Data Cleaning Function
def clean_text(text):
    text = re.sub(r'\W', ' ', text)  # Remove punctuation
    text = text.lower()  # Convert to lowercase
    text = ' '.join([word for word in text.split() if word not in stopwords.words('english')])  # Remove stopwords
    return text

# Apply cleaning
data['text'] = data['text'].apply(clean_text)

# Encode labels
data['label_num'] = data['label'].map({'ham': 0, 'spam': 1})

# Split the dataset
train_data, temp_data = train_test_split(data, test_size=0.2, random_state=42)
val_data, test_data = train_test_split(temp_data, test_size=0.5, random_state=42)

# Save prepared data as DataFrames
train_data.to_pickle('../data/train_data.pkl')
val_data.to_pickle('../data/val_data.pkl')
test_data.to_pickle('../data/test_data.pkl')

print("Data preparation complete! Datasets saved as train_data.pkl, val_data.pkl, and test_data.pkl.")