import streamlit as st
from langchain.chat_models import ChatOpenAI
from langchain.schema import (SystemMessage, HumanMessage, AIMessage)
from langchain.callbacks import get_openai_callback
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse


# WEBサイト要約
def web_summarize(llm):

    def get_url_input():
        url = st.text_input("URL: ", key="input")
        return url


    def validate_url(url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False


    def get_content(url):
        try:
            with st.spinner("Fetching Content ..."):
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                # fetch text from main (change the below code to filter page)
                if soup.main:
                    return soup.main.get_text()
                elif soup.article:
                    return soup.article.get_text()
                else:
                    return soup.body.get_text()
        except:
            st.write('something wrong')
            return None


    def build_prompt(content, n_chars=300):
        return f"""以下はとある。Webページのコンテンツである。内容を{n_chars}程度でわかりやすく要約してください。

    ========

    {content[:1000]}

    ========

    日本語で書いてね！
    """


    def get_answer(llm, messages):
        with get_openai_callback() as cb:
            answer = llm(messages)
        return answer.content, cb.total_cost

    clear_button = st.sidebar.button("Clear chat history", key="clear")
    container = st.container()
    response_container = st.container()

    with container:
        url = get_url_input()
        is_valid_url = validate_url(url)
        if not is_valid_url:
            st.write('Please input valid url')
            answer = None
        else:
            content = get_content(url)
            if content:
                prompt = build_prompt(content)
                st.session_state.messages.append(HumanMessage(content=prompt))
                with st.spinner("ChatGPT is typing ..."):
                    answer, cost = get_answer(llm, st.session_state.messages)
                st.session_state.costs.append(cost)
            else:
                answer = None

        if answer:
            with response_container:
                st.markdown("## Summary")
                st.write(answer)
                st.markdown("---")
                st.markdown("## Original Text")
                st.write(content)

        costs = st.session_state.get('costs', [])
        st.sidebar.markdown("## Costs")
        st.sidebar.markdown(f"**Total cost: ${sum(costs):.5f}**")
        for cost in costs:
            st.sidebar.markdown(f"- ${cost:.5f}")


# YOUTUBE要約
def youtube_summarize(llm):
    clear_button = st.sidebar.button("Clear chat history", key="clear")
    if clear_button or "messages" not in st.session_state:
        st.session_state.messages = [
            SystemMessage(content="YOUTUBEの要約ができます")
        ]
    
    # ユーザーの入力を監視
    if user_input := st.chat_input("YOUTUBEのURLを入力してください"):
        st.session_state.messages.append(HumanMessage(content=user_input))
        with st.spinner("ChatGPT が考えています ..."):
            response = llm(st.session_state.messages)
        st.session_state.messages.append(AIMessage(content=response.content))


# PDF質問
def pdf_question(llm):
    clear_button = st.sidebar.button("Clear chat history", key="clear")
    if clear_button or "messages" not in st.session_state:
        st.session_state.messages = [
            SystemMessage(content="添付したPDFの質問ができます")
        ]
    
    # ユーザーの入力を監視
    if user_input := st.chat_input("PDFを添付してください"):
        st.session_state.messages.append(HumanMessage(content=user_input))
        with st.spinner("ChatGPT が考えています ..."):
            response = llm(st.session_state.messages)
        st.session_state.messages.append(AIMessage(content=response.content))