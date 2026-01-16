import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from wordcloud import WordCloud

df = pd.read_csv('twitter_dataset.csv')
tweets = df['Tweet'].dropna()

# Keyword Extraction
vectorizer = CountVectorizer(stop_words='english', max_features=20)
X = vectorizer.fit_transform(tweets)
print(f"Keywords: {vectorizer.get_feature_names_out()}")

# Topic Modeling using pyLDA
lda = LatentDirichletAllocation(n_components=3, random_state=42)
lda.fit(X)

words = vectorizer.get_feature_names_out()
for i, topic in enumerate(lda.components_):
    print(f"Topic {i}: ", [words[i] for i in topic.argsort()[:-6:-1]])