import streamlit as st
import google.generativeai as genai
from dotenv import load_dotenv
import os
import pandas as pd
from fuzzywuzzy import fuzz
from datasets import load_dataset

# Load environment variables from .env file
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Configuration Check
if not GEMINI_API_KEY:
    st.error("⚠️ ERROR: Gemini API key not set in .env file!")
    st.stop()

# Configure Gemini API
genai.configure(api_key=GEMINI_API_KEY)

try:
    model = genai.GenerativeModel("gemini-1.5-pro")
except Exception as e:
    st.error(f"⚠️ ERROR: Failed to initialize Gemini model - {str(e)}")
    st.stop()

# Load SimpleQA dataset
@st.cache_data
def load_simpleqa_dataset():
    try:
        # Load the dataset with the correct split
        ds = load_dataset("basicv8vc/SimpleQA", split="test")
        df = pd.DataFrame(ds)
        
        # Explicitly rename columns for this specific dataset
        column_mapping = {
            "problem": "question",
            "answer": "answer"
        }
        
        # Apply renaming
        df = df.rename(columns=column_mapping)
        
        # Verify required columns after renaming
        if "question" not in df.columns or "answer" not in df.columns:
            raise ValueError("Dataset must have 'question' and 'answer' columns after renaming")
        
        return df[["question", "answer"]]
    except Exception as e:
        st.error(f"Error loading dataset: {str(e)}")
        return None

dataset = load_simpleqa_dataset()
if dataset is None:
    st.stop()

# Find best matching question in dataset
def find_best_match(user_query, dataset, threshold=80):
    best_match = None
    best_score = 0
    best_answer = None
    for _, row in dataset.iterrows():
        score = fuzz.token_sort_ratio(user_query.lower(), row["question"].lower())
        if score > best_score:
            best_score = score
            best_match = row["question"]
            best_answer = row["answer"]
    if best_score >= threshold:
        return best_answer, best_match, best_score
    return None, best_match, best_score

# Query Gemini API with RAG context
def query_gemini_with_rag(user_query, similar_question=None):
    try:
        # Construct prompt with RAG context if similar question exists
        if similar_question:
            prompt = (
                f"Answer the following query: '{user_query}'.\n"
                f"Context: A similar question found is '{similar_question}'.\n"
                f"Provide a precise and accurate answer."
            )
        else:
            prompt = f"Answer the following query: '{user_query}'.\nProvide a precise and accurate answer."
        response = model.generate_content(prompt)
        if hasattr(response, "text"):
            return response.text
        else:
            return "Unexpected API response format"
    except Exception as e:
        return f"Error querying Gemini API: {str(e)}"

# Streamlit App
st.title("SimpleQA Chatbot")

# Initialize session state for chat history
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Sidebar for chat history
with st.sidebar:
    st.header("Chat History")
    if st.session_state.chat_history:
        for idx, chat in enumerate(st.session_state.chat_history):
            st.write(f"**You**: {chat['prompt'][:30]}{'...' if len(chat['prompt']) > 30 else ''}")
            st.write(f"**Bot**: {chat['response'][:30]}{'...' if len(chat['response']) > 30 else ''}")
            st.write(f"**Source**: {chat['source']}")
            st.markdown("---")
    else:
        st.write("No chats yet.")

# Input prompt
prompt = st.text_input("Enter your query:")

# Send prompt and display response
if prompt:
    with st.spinner("Generating response..."):
        # Check dataset for answer
        answer, matched_question, match_score = find_best_match(prompt, dataset)
        source = "Dataset" if answer else "Gemini API (RAG)"

        if answer:
            response_text = answer
        else:
            # Fallback to Gemini API with RAG
            response_text = query_gemini_with_rag(prompt, matched_question)

        # Store in chat history
        st.session_state.chat_history.append({
            "prompt": prompt,
            "response": response_text,
            "source": source
        })

        # Display the latest response
        st.write(f"**Response**: {response_text}")
        st.write(f"**Source**: {source}")
        if matched_question and source == "Gemini API (RAG)":
            st.write(f"**Similar Question Found**: {matched_question} (Score: {match_score})")