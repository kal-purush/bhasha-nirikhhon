import streamlit as st
from langchain.chat_models import ChatOpenAI
from langchain.schema import (SystemMessage, HumanMessage, AIMessage)


# WEBサイト要約
def web_summarize(llm):
    clear_button = st.sidebar.button("Clear chat history", key="clear")
    if clear_button or "messages" not in st.session_state:
        st.session_state.messages = [
            SystemMessage(content="WEBサイトの要約ができます")
        ]
    st.session_state.cost = []
    
    # ユーザーの入力を監視
    if user_input := st.chat_input("WEBサイトのURLを入力してください"):
        st.session_state.messages.append(HumanMessage(content=user_input))
        with st.spinner("ChatGPT が考えています ..."):
            response = llm(st.session_state.messages)
        st.session_state.messages.append(AIMessage(content=response.content))

    
    # チャット履歴の表示
    messages = st.session_state.get('messages', [])
    for message in messages:
        if isinstance(message, AIMessage):
            with st.chat_message('assistant'):
                st.markdown(message.content)
        elif isinstance(message, HumanMessage):
            with st.chat_message('user'):
                st.markdown(message.content)
        else:  # isinstance(message, SystemMessage):
            st.write(f"System message: {message.content}")


# YOUTUBE要約
def youtube_summarize(llm):
    clear_button = st.sidebar.button("Clear chat history", key="clear")
    if clear_button or "messages" not in st.session_state:
        st.session_state.messages = [
            SystemMessage(content="YOUTUBEの要約ができます")
        ]
    st.session_state.cost = []
    
    # ユーザーの入力を監視
    if user_input := st.chat_input("YOUTUBEのURLを入力してください"):
        st.session_state.messages.append(HumanMessage(content=user_input))
        with st.spinner("ChatGPT が考えています ..."):
            response = llm(st.session_state.messages)
        st.session_state.messages.append(AIMessage(content=response.content))

    
    # チャット履歴の表示
    messages = st.session_state.get('messages', [])
    for message in messages:
        if isinstance(message, AIMessage):
            with st.chat_message('assistant'):
                st.markdown(message.content)
        elif isinstance(message, HumanMessage):
            with st.chat_message('user'):
                st.markdown(message.content)
        else:  # isinstance(message, SystemMessage):
            st.write(f"System message: {message.content}")


# PDF質問
def pdf_question(llm):
    clear_button = st.sidebar.button("Clear chat history", key="clear")
    if clear_button or "messages" not in st.session_state:
        st.session_state.messages = [
            SystemMessage(content="添付したPDFの質問ができます")
        ]
    st.session_state.cost = []
    
    # ユーザーの入力を監視
    if user_input := st.chat_input("PDFを添付してください"):
        st.session_state.messages.append(HumanMessage(content=user_input))
        with st.spinner("ChatGPT が考えています ..."):
            response = llm(st.session_state.messages)
        st.session_state.messages.append(AIMessage(content=response.content))

    
    # チャット履歴の表示
    messages = st.session_state.get('messages', [])
    for message in messages:
        if isinstance(message, AIMessage):
            with st.chat_message('assistant'):
                st.markdown(message.content)
        elif isinstance(message, HumanMessage):
            with st.chat_message('user'):
                st.markdown(message.content)
        else:  # isinstance(message, SystemMessage):
            st.write(f"System message: {message.content}")
    