# import library
import pandas as pd

import nltk
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
from nltk.tokenize import word_tokenize
from nltk.tokenize import sent_tokenize
from nltk.stem import WordNetLemmatizer
# from nltk.corpus import stopwords
import Sastrawi
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
# from Sastrawi import POSTagger
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
import string
from imblearn.over_sampling import SMOTE
import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score, GridSearchCV



# import raw dataset for preprocessing
df = pd.read_csv('train_preprocess.tsv.txt',
                 sep ='\t',
                 names = ["text", "label"])

# remove special characters

def text_cleansing(text):
  # Bersihkan tanda baca (selain huruf dan angka)
  clean_text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
  # Convert to lowercase
  clean_text = clean_text.lower()

  # Remove specific characters properly escaped
  clean_text = re.sub(r'\\xf0\\S+', '', clean_text)
  clean_text = re.sub(r'\\xe\\S+', '', clean_text)

  # Remove repeated characters
  clean_text = re.sub(r'(.)\\1+', r'\1', clean_text)
  # repeated character
  clean_text = re.sub(r'(.)\1+', r'\1', clean_text)

  # Strip leading and trailing spaces
  clean_text = clean_text.strip()

  return clean_text

df['clean_text'] = df.text.apply (text_cleansing)
df.head()

# Create a function to tokenize the text
def tokenize(text):
  words = nltk.word_tokenize(text)
  return words

# Create a function to tokenize the text using word_tokenize
def tokenize_sent(text):
  sent = nltk.sent_tokenize(text)
  return sent



# Apply the tokenization function to `clean_text` column
df["tokens"] = df["clean_text"].apply(tokenize)
df["tokens_sent"] = df["clean_text"].apply(tokenize_sent)


# get Indonesian stopword
stop_words = nltk.corpus.stopwords.words("indonesian")

# function to remove stop words
def filter_stopwords (tokens) :
    filtered_tokens = [token for token in tokens if token not in stop_words]
    return filtered_tokens

# Remove the stop words from the tokens
df["filtered_tokens"] = df["tokens"].apply(filter_stopwords)

# Initialize Sastrawi Stemmer
stemmer_factory = StemmerFactory()
stemmer = stemmer_factory.create_stemmer()

# Define a function preproses tokens to be a preprossesed text
def stemming(tokens):
 stemmed_tokens = [stemmer.stem(token) for token in tokens]
 return ' '.join(stemmed_tokens)   


#preprosseing
##  (text_cleansing)x
## tokenize x
## filter stopword x
## preprocess_text (stemming) x
## test = tfidf_vectorizer.fit_transform(hasil_baru)

# df['preprocessed_text'] = df['filtered_tokens'].apply(stemming)

    ## TRAINING

# import preprocessed dataset 
df_ready = pd.read_csv("sentiment_analysis_dataset_filtered_tokens.csv")
df_ready['preprocessed_text'] = df_ready['preprocessed_text'].fillna('')

# feature extraction
# perpare preprocessed and tokenized text data
tokenized_texts = df_ready['preprocessed_text']

# Initialize the TF-IDF vectorizer
tfidf_vectorizer = TfidfVectorizer()

# Fit and transform the data to obtain TF-IDF features from text
tfidf_features = tfidf_vectorizer.fit_transform(tokenized_texts)

# Split train and test
X_train, X_test, y_train, y_test = train_test_split(tfidf_features, df['label'], test_size=0.2, random_state=42)

# tackle imbalance 
smote = SMOTE(random_state=42)
X_train_smote, y_train_smote = smote.fit_resample(X_train, y_train)

# Training with logistic regresion
# Create a LogisticRegression classifier
logreg_classifier = LogisticRegression(max_iter=100)

# Fit the model on the training data
logreg_classifier.fit(X_train_smote, y_train_smote)

# Predict on the test data
predictions_logreg = logreg_classifier.predict(X_test)

# Calculate evaluation metrics
accuracy_logreg = accuracy_score(y_test, predictions_logreg)
precision_logreg = precision_score(y_test, predictions_logreg, average='weighted')
recall_logreg = recall_score(y_test, predictions_logreg, average='weighted')
f1_logreg = f1_score(y_test, predictions_logreg, average='weighted')

# Define the logistic regression model
logreg_model = LogisticRegression()

# Define the range of hyperparameters
param_grid = {
    'C': [0.01, 0.1, 1, 10, 100],  # regularization strength
    'penalty': ['5','l1', 'l2','15']   # penalty type
}

# hyperparameter tuning logreg
# Perform grid search over hyperparameter grid
grid_search = GridSearchCV(logreg_model, param_grid, cv=5, scoring='accuracy')
grid_search.fit(X_train, y_train)

best_params = grid_search.best_params_
best_score = grid_search.best_score_

tuned_logreg_model = LogisticRegression(C=10, penalty='l2' )

## save and load model
# Save the tuned logistic regression model 
model_logreg_filename = 'tuned_logreg_model.pkl'
with open(model_logreg_filename, 'wb') as model_file:
    joblib.dump(tuned_logreg_model, model_logreg_filename)

# Load the tuned logistic regression model 
with open(model_logreg_filename, 'rb') as model_file:
    loaded_model_logreg = joblib.load(model_logreg_filename)


## save and load tfidf_vectorizer
# Save the tfidf_vectorizer used for training
tfidf_vectorizer_filename = 'tfidf_vectorizer.pkl'
with open(tfidf_vectorizer_filename, 'wb') as vectorizer_file:
   joblib.dump(tfidf_vectorizer, vectorizer_file)

# Load the tfidf_vectorizer used for training
with open(tfidf_vectorizer_filename, 'rb') as vectorizer_file:
   loaded_tfidf_vectorizer = joblib.load(tfidf_vectorizer_filename)

# Fit the loaded model on the training data
loaded_model_logreg.fit(X_train_smote, y_train_smote)

def text_processing(text):
   processed_text = text_cleansing(text)
   processed_text = nltk.word_tokenize(processed_text)
   processed_text = filter_stopwords(processed_text)
   processed_text = stemming(processed_text)
   return processed_text

def predict_NN (text):
   tfidf_vector = loaded_tfidf_vectorizer.transform([text])  
   sentiment = loaded_model_logreg.predict(tfidf_vector)
   return sentiment 

# Print the predicted label

# print(f"sentimennya adalah : {predict_NN(new_text_processed)}")




