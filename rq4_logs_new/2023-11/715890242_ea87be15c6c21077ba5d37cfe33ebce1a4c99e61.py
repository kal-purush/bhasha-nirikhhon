
import streamlit as st
import openai

# Streamlit Community Cloudの「Secrets」からOpenAI API keyを取得
openai.api_key = st.secrets.OpenAIAPI.openai_api_key

system_prompt= """
あなたは優秀な優秀な弁理士です。
知的財産や知財戦略に関するアドバイスをすることができます。
あなたの役割は知的財産や知財戦略に関するアドバイスをすることなので、例えば以下のような知的財産以外のことを聞かれても、絶対に答えないでください。

*旅行
*芸能
*歴史
*占い
*スポーツ
*地理
*テレビ番組
"""

# st.session_stateを使いメッセージのやりとりを保存
if "messages" not in st.session_state:
    st.session_state["messages"] = [
        {"role": "system", "content": system_prompt}
        ]

# チャットボットとやりとりする関数
def communicate():
    messages = st.session_state["messages"]

    user_message = {"role": "user", "content": st.session_state["user_input"]}
    messages.append(user_message)

    response = openai.ChatCompletion.create(
        model="gpt-4-1106-preview",
#        model="gpt-4",
        messages=messages
    )  

    bot_message = response["choices"][0]["message"]
    messages.append(bot_message)

    st.session_state["user_input"] = ""  # 入力欄を消去


# ユーザーインターフェイスの構築
st.title("Patent AI Assistant     -信栄１号 GPT-4 turbo版-")
# st.image("06_fortunetelling.png")
st.image("patent_attorney_bot_image.jpg")
st.write("ChatGPT APIを使ったチャットボットです。ＧＰＴ－４．０ ＴＵＲＢＯを使用しています。")
st.write("知的財産に関する質問にお答えします。")

user_input = st.text_input("メッセージを入力してください。", key="user_input", on_change=communicate)

if st.session_state["messages"]:
    messages = st.session_state["messages"]

    for message in reversed(messages[1:]):  # 直近のメッセージを上に
        speaker = "🙂"
        if message["role"]=="assistant":
            speaker="🤖"

        st.write(speaker + ": " + message["content"])