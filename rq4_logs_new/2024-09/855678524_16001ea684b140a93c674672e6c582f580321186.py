import streamlit as st
from audiorecorder import audiorecorder
from openai import OpenAI
import os
from datetime import datetime
from gtts import gTTS
import base64

# OpenAI �대씪�댁뼵�� 珥덇린��
client = None

##### 湲곕뒫 援ы쁽 �⑥닔 #####
def STT(audio):
    # �뚯씪 ����
    filename = 'input.mp3'
    audio.export(filename, format="mp3")
    # �뚯썝 �뚯씪 �닿린
    audio_file = open(filename, "rb")
    # Whisper 紐⑤뜽�� �쒖슜�� �띿뒪�� �산린
    transcript = client.audio.transcriptions.create(model="whisper-1", file=audio_file)
    
    audio_file.close()
    # �뚯씪 ��젣
    os.remove(filename)
    return transcript.text

def ask_gpt(prompt, model):
    response = client.chat.completions.create(model=model, messages=prompt)
    system_message = response.choices[0].message
    return system_message.content

def TTS(response):
    # gTTS 瑜� �쒖슜�섏뿬 �뚯꽦 �뚯씪 �앹꽦
    filename = "output.mp3"
    tts = gTTS(text=response, lang="ko")
    tts.save(filename)

    # �뚯썝 �뚯씪 �먮룞 �ъ깮
    with open(filename, "rb") as f:
        data = f.read()
        b64 = base64.b64encode(data).decode()
        md = f"""
            <audio autoplay="True">
            <source src="data:audio/mp3;base64,{b64}" type="audio/mp3">
            </audio>
            """
        st.markdown(md, unsafe_allow_html=True)
    # �뚯씪 ��젣
    os.remove(filename)

##### 硫붿씤 �⑥닔 #####
def main():
    global client  # OpenAI �대씪�댁뼵�몃� �꾩뿭�먯꽌 �ъ슜�� �� �덈룄濡� �ㅼ젙

    # 湲곕낯 �ㅼ젙
    st.set_page_config(page_title="�뚯꽦 鍮꾩꽌 �꾨줈洹몃옩", layout="wide")

    # session state 珥덇린��
    if "chat" not in st.session_state:
        st.session_state["chat"] = []

    if "messages" not in st.session_state:
        st.session_state["messages"] = [{"role": "system", "content": "You are a thoughtful assistant. Respond to all input in 25 words and answer in korean"}]

    if "check_reset" not in st.session_state:
        st.session_state["check_reset"] = False

    # �쒕ぉ 
    st.header("�뚯꽦 鍮꾩꽌 �꾨줈洹몃옩")
    st.markdown("---")

    # 湲곕낯 �ㅻ챸
    with st.expander("�뚯꽦鍮꾩꽌 �꾨줈洹몃옩�� 愿��섏뿬", expanded=True):
        st.write(
        """     
        - �뚯꽦鍮꾩꽌 �꾨줈洹몃옩�� UI�� �ㅽ듃由쇰┸�� �쒖슜�덉뒿�덈떎.
        - STT(Speech-To-Text)�� OpenAI�� Whisper AI瑜� �쒖슜�덉뒿�덈떎. 
        - �듬��� OpenAI�� GPT 紐⑤뜽�� �쒖슜�덉뒿�덈떎. 
        - TTS(Text-To-Speech)�� 援ш��� Google Translate TTS瑜� �쒖슜�덉뒿�덈떎.
        """
        )

    # �ъ씠�쒕컮 �앹꽦
    with st.sidebar:
        # Open AI API �� �낅젰諛쏄린
        api_key = st.text_input(label="OPENAI API ��", placeholder="Enter Your API Key", value="", type="password")

        if api_key:
            # OpenAI �대씪�댁뼵�� �ㅼ젙
            os.environ["OPENAI_API_KEY"] = api_key  # �섍꼍 蹂��섏뿉 API �� ����
            client = OpenAI(api_key=api_key)  # �대씪�댁뼵�� �앹꽦

        st.markdown("---")

        # GPT 紐⑤뜽�� �좏깮�섍린 �꾪븳 �쇰뵒�� 踰꾪듉 �앹꽦
        model = st.radio(label="GPT 紐⑤뜽", options=["gpt-4", "gpt-3.5-turbo"])

        st.markdown("---")

        # 由ъ뀑 踰꾪듉 �앹꽦
        if st.button(label="珥덇린��"):
            st.session_state["chat"] = []
            st.session_state["messages"] = [{"role": "system", "content": "You are a thoughtful assistant. Respond to all input in 25 words and answer in korean"}]
            st.session_state["check_reset"] = True

    # 湲곕뒫 援ы쁽 怨듦컙
    col1, col2 = st.columns(2)
    with col1:
        # �쇱そ �곸뿭 �묒꽦
        st.subheader("吏덈Ц�섍린")
        
        # �뚯꽦 �뱀쓬 �꾩씠肄� 異붽�
        audio = audiorecorder("�대┃�섏뿬 �뱀쓬�섍린", "�뱀쓬以�...")
        
        # '�대┃�섏뿬 �뱀쓬�섍린' 踰꾪듉�� �뚮졇�� �뚮쭔 API �� 泥댄겕
        if (audio.duration_seconds > 0) and (st.session_state["check_reset"] == False):
            if not api_key:
                st.error("API �ㅻ� �낅젰�� 二쇱꽭��.")  # API �ㅺ� �놁쑝硫� 寃쎄퀬 硫붿떆吏� 異쒕젰
            else:
                # �뱀쓬�� �ㅻ뵒�ㅻ� 泥섎━
                st.audio(audio.export().read())
                try:
                    question = STT(audio)  # STT �⑥닔 �몄텧
                    now = datetime.now().strftime("%H:%M")
                    st.session_state["chat"] = st.session_state["chat"] + [("user", now, question)]
                    st.session_state["messages"] = st.session_state["messages"] + [{"role": "user", "content": question}]
                except Exception as e:
                    st.error(f"STT 蹂��� 以� �ㅻ쪟媛� 諛쒖깮�덉뒿�덈떎: {e}")

    with col2:
        # �ㅻⅨ履� �곸뿭 �묒꽦
        st.subheader("吏덈Ц/�듬�")
        if (audio.duration_seconds > 0) and (st.session_state["check_reset"] == False) and api_key:
            response = ask_gpt(st.session_state["messages"], model)
            st.session_state["messages"] = st.session_state["messages"] + [{"role": "system", "content": response}]
            now = datetime.now().strftime("%H:%M")
            st.session_state["chat"] = st.session_state["chat"] + [("bot", now, response)]

            for sender, time, message in st.session_state["chat"]:
                if sender == "user":
                    st.write(f'<div style="display:flex;align-items:center;"><div style="background-color:#007AFF;color:white;border-radius:12px;padding:8px 12px;margin-right:8px;">{message}</div><div style="font-size:0.8rem;color:gray;">{time}</div></div>', unsafe_allow_html=True)
                else:
                    st.write(f'<div style="display:flex;align-items:center;justify-content:flex-end;"><div style="background-color:lightgray;border-radius:12px;padding:8px 12px;margin-left:8px;">{message}</div><div style="font-size:0.8rem;color:gray;">{time}</div></div>', unsafe_allow_html=True)

            TTS(response)
        else:
            st.session_state["check_reset"] = False

if __name__ == "__main__":
    main()