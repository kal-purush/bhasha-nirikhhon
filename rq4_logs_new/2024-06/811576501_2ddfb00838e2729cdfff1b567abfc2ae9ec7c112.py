import streamlit as st
# import torch
# from transformers import AutoTokenizer, AutoModelForSequenceClassification
# from scipy.special import softmax
import pandas as pd
import random

# MODEL = "cardiffnlp/twitter-roberta-base-sentiment"
# tokenizer = AutoTokenizer.from_pretrained(MODEL)
# model = AutoModelForSequenceClassification.from_pretrained(MODEL)

# # Define function to apply to input example
# def polarity_scores_roberta(example):
#     encoded_text = tokenizer(example, return_tensors='pt')
#     output = model(**encoded_text)
#     scores = output[0][0].detach().numpy()
#     scores = softmax(scores)
#     scores_dict = {
#         'Negative': scores[0],
#         'Neutral': scores[1],
#         'Positive': scores[2]
#     }

#     # Find the key with the highest score
#     highest_score_key = max(scores_dict, key=scores_dict.get)

#     return highest_score_key

# Streamlit App

# Sidebar
with st.sidebar:
    st.title('Sentiment Analysis')
    st.markdown("Let's you find out \
        what your customers **like** and **dislike**\
            and how you can make a product **better** for them.")
    st.markdown("You can analyse **customer reviews**,\
        open-ended **customer survey** questions,\
        threads in discussions,\
        social media posts - As long as it's digital text.")

    st.markdown("Allows to refine your marketing,\
         optimize product offerings or promotions, \
            and identify emerging trends.")
    

st.title("Customer Review Analysis")
# Function to select a random text from the DataFrame

df = pd.read_csv('Reviews_clean.csv')

with st.container():
    def select_random_text():
        random_index = random.randint(0, len(df) - 1)
        return df.iloc[random_index]['Text']

    # Button to trigger random selection and display
    if st.button("1. Click to Select Random Review"):
        random_text = select_random_text()
        st.write(random_text)
        
        # Filter the DataFrame to display rows with the selected 'Text'
        filtered_df = df[df['Text'] == random_text]
        
        # Display the filtered DataFrame in a table
        st.write("Customer Survey Subset:")
        st.dataframe(filtered_df[['Text', 'Summary', 'ProfileName', 'UserId']])

from transformers import pipeline

sent_pipeline = pipeline("sentiment-analysis")

with st.container():
    example = st.text_area("Enter a Customer Review")
    if st.button("2. Analyze"):
        result = sent_pipeline(example)
        label = result[0]['label']
        st.write(f"Sentiment: {label}")

col1, col2, col3 = st.columns(3)
col2.markdown('Made with ❤️ by [Alejandro](https://github.com/aleivaar94)')

