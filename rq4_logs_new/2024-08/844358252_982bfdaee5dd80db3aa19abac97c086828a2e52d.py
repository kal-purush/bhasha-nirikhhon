import streamlit as st
from yolo_chat import yolo_object_detection_and_chat
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

# Initialize session state for chat history
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Streamlit app UI
st.title("AI Assistant")

# Sidebar for selecting functionality
option = st.sidebar.selectbox(
    "Choose a functionality",
    ("Chat with Document Embeddings", "Object Detection with YOLOv8")
)

if option == "Chat with Document Embeddings":
    st.header("Chat with Document Embeddings")
    
    from langchain_groq import ChatGroq
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    from langchain.chains.combine_documents import create_stuff_documents_chain
    from langchain_core.prompts import ChatPromptTemplate
    from langchain.chains import create_retrieval_chain
    from langchain_community.vectorstores import FAISS
    from langchain_community.document_loaders import PyPDFDirectoryLoader
    from langchain_google_genai import GoogleGenerativeAIEmbeddings

    groq_api_key = os.getenv('GROQ_API_KEY')
    os.environ["GOOGLE_API_KEY"] = os.getenv("GOOGLE_API_KEY")

    # Initialize the LLM
    llm = ChatGroq(groq_api_key=groq_api_key, model_name="Llama3-8b-8192")

    # Create the prompt template
    prompt = ChatPromptTemplate.from_template(
        """
        Answer the questions based on the provided context only.
        Please provide the most accurate response based on the question.
        <context>
        {context}
        <context>
        Questions: {input}
        """
    )

    # Function to create vector embeddings from documents
    def vector_embedding():
        if "vectors" not in st.session_state:
            st.session_state.embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
            st.session_state.loader = PyPDFDirectoryLoader("./document")  # Data Ingestion
            st.session_state.docs = st.session_state.loader.load()  # Document Loading
            st.session_state.text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)  # Chunk Creation
            st.session_state.final_documents = st.session_state.text_splitter.split_documents(st.session_state.docs[:20])  # Splitting
            st.session_state.vectors = FAISS.from_documents(st.session_state.final_documents, st.session_state.embeddings)  # Vector OpenAI embeddings

    # Display the chat history at the top
    if st.session_state.chat_history:
        for i, chat in enumerate(st.session_state.chat_history):
            st.write(f"*You:* {chat['user']}")
            st.write(f"*AI:* {chat['response']}")
            st.write(f"*Response time:* {chat['response_time']} seconds")
            st.write("---")

    # Create a placeholder for the chat input at the bottom
    chat_input_placeholder = st.empty()

    # Render the chat input in the placeholder
    with chat_input_placeholder.container():
        prompt1 = st.text_input("Enter Your Question from Documents")

        if st.button("Create Document Embeddings"):
            vector_embedding()
            st.write("Vector Store DB is ready")

        if prompt1:
            document_chain = create_stuff_documents_chain(llm, prompt)
            retriever = st.session_state.vectors.as_retriever()
            retrieval_chain = create_retrieval_chain(retriever, document_chain)
            
            start = time.process_time()
            response = retrieval_chain.invoke({'input': prompt1})
            response_time = time.process_time() - start

            # Store the conversation in session state
            st.session_state.chat_history.append({"user": prompt1, "response": response['answer'], "response_time": response_time})

            # Rerun the script to display the updated chat history
            st.experimental_rerun()

elif option == "Object Detection with YOLOv8":
    st.header("Object Detection with YOLOv8")
    yolo_object_detection_and_chat()

# Add a "Clear Chat" button to reset the conversation
if st.button("Clear Chat"):
    st.session_state.chat_history = []
    st.experimental_rerun()