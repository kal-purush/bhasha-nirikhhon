# app.py
import streamlit as st
from rag_model import CustomerSupportRAG
import datetime

# Initialize the RAG model
@st.cache_resource
def get_rag_model():
    return CustomerSupportRAG()

rag_model = get_rag_model()

# Set up the Streamlit page
st.title("Customer Support Assistant")
st.write("Ask questions about our products or services!")

# Initialize session state for conversation history
if "messages" not in st.session_state:
    st.session_state.messages = []
    
if "escalated" not in st.session_state:
    st.session_state.escalated = False

# Display conversation history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])
        
        # Display timestamp
        st.caption(f"{message['timestamp']}")

# Handle user input
if not st.session_state.escalated:
    if prompt := st.chat_input("Type your question here..."):
        # Add user message to chat history
        timestamp = datetime.datetime.now().strftime("%I:%M %p")
        st.session_state.messages.append({"role": "user", "content": prompt, "timestamp": timestamp})
        
        # Display user message
        with st.chat_message("user"):
            st.write(prompt)
            st.caption(timestamp)
        
        # Get response from RAG model
        with st.chat_message("assistant"):
            response_placeholder = st.empty()
            response_data = rag_model.get_response(prompt)
            
            response_placeholder.write(response_data["response"])
            timestamp = datetime.datetime.now().strftime("%I:%M %p")
            st.caption(timestamp)
            
            # Add assistant message to chat history
            st.session_state.messages.append({"role": "assistant", "content": response_data["response"], "timestamp": timestamp})
            
            # Handle escalation
            if response_data["escalate"]:
                st.session_state.escalated = True
                st.rerun()
else:
    # Display human support interface
    st.info("You've been connected to human support. Please wait for an agent to respond.")
    
    # Human agent simulation interface (in a real app, this would be a separate admin panel)
    with st.expander("Human Agent Interface (Demo Only)"):
        if agent_response := st.text_area("Agent Response:"):
            if st.button("Send Response"):
                # Add agent message to chat history
                timestamp = datetime.datetime.now().strftime("%I:%M %p")
                st.session_state.messages.append({"role": "assistant", "content": agent_response, "timestamp": timestamp})
                
                # Reset escalation flag
                st.session_state.escalated = False
                st.rerun()
        
        if st.button("Return to Automated Support"):
            st.session_state.escalated = False
            st.rerun()