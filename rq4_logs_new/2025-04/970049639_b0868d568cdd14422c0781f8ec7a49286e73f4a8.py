"""
Healthcare Database Request Assistant

This application helps healthcare professionals process database-level service requests
that cannot be handled through the user interface.

Author: Raja
Date: April 2025
"""

import os
import streamlit as st
from dotenv import load_dotenv

# Import components and utilities
from src.utils.config import PAGE_TITLE, PAGE_ICON, PAGE_LAYOUT, SIDEBAR_STATE, DATA_DIR
from src.utils.styles import CUSTOM_CSS
from src.utils.api_validator import validate_api_key
from src.components.header import render_header
from src.components.sidebar import render_sidebar
from src.components.main_content import render_input_section, display_response, display_history
from src.components.api_key_form import render_api_key_form
from src.data_handlers.document_loader import (
    load_documents, process_documents, create_vectorstore,
    load_vectorstore, reset_vectorstore
)
from src.models.llm_chain import create_retrieval_chain_with_vectorstore


def main():
    """
    Main function to run the Healthcare Database Request Assistant.
    """
    # Configure the page first (needs to be at the top)
    st.set_page_config(
        page_title=PAGE_TITLE,
        page_icon=PAGE_ICON,
        layout=PAGE_LAYOUT,
        initial_sidebar_state=SIDEBAR_STATE
    )

    # Apply custom CSS
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    # Initialize session state for history and API key status
    if 'history' not in st.session_state:
        st.session_state.history = []
    if 'api_key_valid' not in st.session_state:
        st.session_state.api_key_valid = False

    # Load environment variables
    load_dotenv()

    # Check if API key is available and valid
    api_key = os.getenv("GOOGLE_API_KEY")

    # If we have an API key in the environment, validate it
    if api_key and not st.session_state.api_key_valid:
        is_valid, error_message = validate_api_key(api_key)
        if not is_valid:
            # If the API key is invalid, clear it and show the form
            st.error(f"Invalid API key in environment: {error_message}")
            api_key = None
            # Clear the environment variable to prevent reusing the invalid key
            os.environ.pop("GOOGLE_API_KEY", None)

    # If API key is not available or invalid, and not already validated in this session
    if not api_key and not st.session_state.api_key_valid:
        # Show the API key form and get the result
        api_key_provided = render_api_key_form()

        # If the API key was provided and validated, mark it as valid for this session
        if api_key_provided:
            st.session_state.api_key_valid = True
            st.rerun()  # Rerun the app to use the new API key

        # If API key is still not provided or invalid, don't proceed with the rest of the app
        return

    # If we get here, we have a valid API key
    st.session_state.api_key_valid = True

    # Render the header
    render_header()

    # Render the sidebar and get reset_index button state
    reset_index = render_sidebar()

    # Reset vector index if requested
    if reset_index:
        if reset_vectorstore():
            st.session_state.pop('vectorstore', None)
            st.rerun()

    # Render the input section
    input_text, submit_button, clear_button = render_input_section()

    # Clear input and response if clear button is clicked
    if clear_button:
        st.rerun()

    # Check if we have a cached vectorstore in session state
    if 'vectorstore' not in st.session_state:
        try:
            # Try to load from disk first
            vectorstore = load_vectorstore()

            # If not available, create a new one
            if vectorstore is None:
                st.info(f"Attempting to load documents")
                docs = load_documents(DATA_DIR)

                if not docs:
                    st.warning(f"No documents found in {DATA_DIR}. Please check the path.")
                else:
                    documents = process_documents(docs)
                    vectorstore = create_vectorstore(documents, save_local=True)

            # Store in session state
            if vectorstore is not None:
                st.session_state.vectorstore = vectorstore
            else:
                # If we couldn't create a vectorstore, create a simple mock one for testing
                st.warning("Could not create a proper vector database. Using a simplified version for testing.")
                # Import necessary modules for creating a mock vectorstore
                from langchain.docstore.document import Document
                from langchain_community.vectorstores import FAISS
                from langchain_huggingface import HuggingFaceEmbeddings

                # Create a simple document
                simple_docs = [Document(page_content="This is a test document for healthcare database requests.")]

                # Try to create a simple vectorstore
                try:
                    simple_embeddings = HuggingFaceEmbeddings(
                        model_name="all-MiniLM-L6-v2",
                        model_kwargs={'device': 'cpu'}
                    )
                    simple_vectorstore = FAISS.from_documents(documents=simple_docs, embedding=simple_embeddings)
                    st.session_state.vectorstore = simple_vectorstore
                    st.info("Using simplified vector database for basic functionality.")
                except Exception as e2:
                    st.error(f"Could not create even a simplified vector database: {str(e2)}")
                    st.error("The application cannot function without a vector database.")
        except Exception as e:
            st.error(f"Error setting up the document database: {str(e)}")
            st.info("Attempting to use a simplified version for testing.")

            # Try to create a simple mock vectorstore as a fallback
            try:
                from langchain.docstore.document import Document
                from langchain_community.vectorstores import FAISS
                from langchain_huggingface import HuggingFaceEmbeddings

                # Create a simple document
                simple_docs = [Document(page_content="This is a test document for healthcare database requests.")]

                # Create a simple vectorstore
                simple_embeddings = HuggingFaceEmbeddings(
                    model_name="all-MiniLM-L6-v2",
                    model_kwargs={'device': 'cpu'}
                )
                simple_vectorstore = FAISS.from_documents(documents=simple_docs, embedding=simple_embeddings)
                st.session_state.vectorstore = simple_vectorstore
                st.info("Using simplified vector database for basic functionality.")
            except Exception as e2:
                st.error(f"Could not create even a simplified vector database: {str(e2)}")
                st.error("The application cannot function without a vector database.")

    # Process the request when the button is clicked
    if submit_button and input_text and 'vectorstore' in st.session_state:
        with st.spinner("Processing your request..."):
            try:
                # Create the retrieval chain
                retrieval_chain = create_retrieval_chain_with_vectorstore(st.session_state.vectorstore)

                # Process the request
                response = retrieval_chain.invoke({"input": input_text})

                # Extract action type for history
                action_type = "Unknown"
                if "Action Type:" in response['answer']:
                    action_start = response['answer'].find("Action Type:") + len("Action Type:")
                    action_end = response['answer'].find("\n", action_start)
                    action_type = response['answer'][action_start:action_end].strip()
                elif "ERROR:" in response['answer']:
                    action_type = "Error"

                # Add to history
                st.session_state.history.append((input_text, action_type))

                # Display the response
                display_response(response)
            except ValueError as e:
                # Handle API key errors
                st.error(f"API Key Error: {str(e)}")
                # Reset API key validation status to force re-entry
                st.session_state.api_key_valid = False
                st.button("Re-enter API Key", on_click=lambda: st.rerun())
            except Exception as e:
                # Handle other errors
                st.error(f"An error occurred: {str(e)}")

    # Display history
    display_history(st.session_state.history)


if __name__ == "__main__":
    main()