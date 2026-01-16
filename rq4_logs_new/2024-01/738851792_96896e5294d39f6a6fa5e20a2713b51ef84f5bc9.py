import json
import streamlit as st
from langchain.chat_models import ChatOpenAI
from langchain.schema import (SystemMessage, HumanMessage, AIMessage)

def main():
    # JSONファイルからAPIキーを読み込む
    with open('/Users/kii/work/python_study/chat_gpt/ai_chat/config.json') as config_file:
        config = json.load(config_file)
        
    # ページ設定
    init_page()

    # モデルの選択
    llm = selet_model(config)
    
    # メッセージの初期化
    init_messages()

    # ユーザーの入力を監視
    if user_input := st.chat_input("聞きたいことを入力してね！"):
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
        
        
def init_page():
    st.set_page_config(
        page_title="My ChatGPT",
        page_icon="⚙️"
    )
    st.header("My ChatGPT")
    st.sidebar.title("ChatGPT")
        
        
def init_messages():
    clear_button = st.sidebar.button("Clear chat history", key="clear")
    if clear_button or "messages" not in st.session_state:
        st.session_state.messages = [
            SystemMessage(content="何かお役に立てることはありますか？")
        ]
    st.session_state.cost = []
        
        
def selet_model(config):
    # サイドバーにモデル選択のラジオボタンを追加
    model = st.sidebar.radio("Choose a model", ["GPT-3.5", "GPT-4"])
    if model == "GPT-3.5":
        model_name = "gpt-3.5-turbo-0613"
    else:
        model_name = "gpt-4"
        
    # サイドバーにスライダーを追加、temperatureの値を選択可能にする
    # 初期値は0.0、最小値は0.0、最大値は2.0、ステップは0.1
    temperature = st.sidebar.slider("Temperature", 0.0, 2.0, 0.0, 0.1)
    
    st.sidebar.markdown("## Costs")
    st.sidebar.markdown("**Total cost**")
    # st.sidebar.markdown(cb.total_cost)

    return ChatOpenAI(temperature=temperature, model_name=model_name)        

if __name__ == '__main__':
    main()