# app.py

import streamlit as st
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

st.title("Multi-Label Intent Classifier")
st.write("Enter a Google Play Store app review below:")

review = st.text_area("App Review")

if st.button("Analyze"):
    # Sentiment
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(review)
    st.write("**Sentiment Analysis:**")
    st.json(sentiment)

    # Intent Prediction placeholder
    st.write("**Predicted Intents:**")
    st.write("Coming soon — intent prediction via XLNet")