import streamlit as st
from utils.process_files import process_files
from utils.qa_chain import user_input
from dotenv import load_dotenv

load_dotenv()

def main():
    st.set_page_config(page_title="Chat PDF & Word", layout="wide")
    st.header("Chat With Multiple PDF and Documents ")

    user_question = st.text_input("Ask a Question from the uploaded Files here")

    if user_question:
        response = user_input(user_question)
        st.write("Reply: ", response)

    with st.sidebar:
        st.title("Menu:")
        uploaded_files = st.file_uploader("Upload PDF/Word Files", type=["pdf", "docx"], accept_multiple_files=True)
        if st.button("Submit & Process"):
            with st.spinner("Processing..."):
                process_files(uploaded_files)
                st.success("Files processed successfully!")

if __name__ == "__main__":
    main()
