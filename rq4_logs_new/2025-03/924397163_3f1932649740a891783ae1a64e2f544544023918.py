import streamlit as st
from sklearn.preprocessing import LabelEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
import time
import pandas as pd
import joblib
from PIL import Image

#Spam Data
#Finally, use the pandas read_csv function to load the extracted file into a pandas dataframe
Spam_df = pd.read_csv("SMSSpamCollection", sep='\t', names=['Label', 'Message'])

# Remove duplicates
Spam_df = Spam_df.drop_duplicates()
X = Spam_df['Message']
Spam_vectorizer = TfidfVectorizer()
X = Spam_vectorizer.fit_transform(X)
Spam_model = joblib.load('SpamModel.joblib')


#Phishing Data
Phishing_df = pd.read_csv("Phishing_Email.csv")
Phishing_df = Phishing_df.iloc[:, 1:] #drop the first column
# Replace null values with a placeholder string (e.g., 'N/A')
Phishing_df['Email Text'].fillna('N/A', inplace=True)
# Remove duplicates
Phishing_df = Phishing_df.drop_duplicates()
x = Phishing_df["Email Text"]
Phishing_vectorizer = TfidfVectorizer()
x = Phishing_vectorizer.fit_transform(x)
Phishing_model = joblib.load('PhishModel.joblib')

# Add custom CSS to change sidebar color
st.markdown(
    """
    <style>
        .sidebar .sidebar-content {
            background-color: #3498db;  /* Choose your desired color */
            color: white;  /* Choose text color for better visibility */
        }
    </style>
    """,
    unsafe_allow_html=True
)
#Streamlit Side
with st.sidebar:
    st.subheader("Choose your Email Filter")
    option = st.selectbox("Choose an option", ["Intro Page", "Spam", "Phishing"])


def predict_spam():
    new_email_vector = Spam_vectorizer.transform([Spam])
    prediction = Spam_model.predict_proba(new_email_vector)
    threshold = 0.5
    my_bar = st.progress(0)
    for percent_complete in range(100):
        time.sleep(0.1)
        my_bar.progress(percent_complete + 1)
    if prediction[0][1] >= threshold:
        st.error("The email is classified as spam.")
    else:
        st.success("The email is classified as safe.")


def predict_phish():
    new_email_vector = Phishing_vectorizer.transform([Phish])
    prediction = Phishing_model.predict(new_email_vector)
    threshold = 0.5
    my_bar = st.progress(0)
    for percent_complete in range(100):
        time.sleep(0.1)
        my_bar.progress(percent_complete + 1)
    if prediction >= threshold:
        st.success("This is a safe email.")
    else:
        st.error("This is a phishing email.")


if option == "Intro Page":
    st.title("Welcome to Email Security Solutions using Machine Learning 🤖")
    text = "In today's interconnected world, emails have become an integral part of our personal and professional lives. While they facilitate quick communication and information exchange, they also open the door to potential threats, two of the most notorious being spam and phishing."
    st.write(text)
    st.subheader("The Dangers of Spam and Phishing\n")
    st.write("**1. Spam:**")
    st.markdown("Unwanted spam emails clog our inboxes, bombarding us with advertisements, scams, and irrelevant content. "
        "Apart from being an annoying distraction, spam can sometimes carry malware or be a breeding ground for phishing attempts.\n")
    st.write("**2. Phishing:**")
    st.write("Phishing attacks are a more sinister form of deception. "
        "Cybercriminals impersonate trustworthy sources, manipulating users into revealing sensitive information such as passwords, credit card numbers, or social security details. "
        "Falling victim to phishing can have severe consequences, including identity theft and financial loss.\n")

    st.subheader("How AI Comes to the Rescue\n")
    st.write("With the rise of these email threats, the need for advanced email security solutions has never been more critical. "
        "This is where AI steps in, with machine learning at its forefront.\n")
    st.write("**1. Intelligent Pattern Recognition:**")
    st.write("Machine learning models are trained to recognize patterns and anomalies in email content. "
        "By analyzing large datasets of emails, they become adept at distinguishing between legitimate messages and spam.\n")
    st.write("**2. Real-Time Analysis:**")
    st.write("AI-powered solutions can scan incoming emails in real time, identifying suspicious elements, including unusual links, deceptive sender addresses, or malicious attachments. "
        "This real-time analysis prevents phishing emails from reaching your inbox.\n")
    st.write("**3. Continuous Learning:**")
    st.write("Machine learning is a dynamic process."
        "As new email threats emerge, these systems adapt and improve their detection capabilities over time, staying one step ahead of cybercriminals.\n")
    st.write("**4. Enhanced User Protection:**")
    st.write("By implementing AI-driven email security, individuals and organizations can shield themselves from potential threats, ensuring confidential data remains secure.\n")

    img = Image.open('images.jpeg')
    st.image(img, use_column_width=True)

    imge = Image.open('NLP.jpg')
    st.image(imge, use_column_width=True)

if option == "Spam":
    st.title("Welcome to the Spam Detector")
    Spam = st.text_input("Please input your email details: \n")
    st.button("Predict", on_click = predict_spam)

if option == "Phishing":
    st.title("Welcome to the Phishing Detector")
    Phish = st.text_input("Please input your email details: \n")
    st.button("Predict", on_click = predict_phish)
