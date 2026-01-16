# 音声を扱う
from pydub import AudioSegment
from pydub.silence import split_on_silence
import speech_recognition as sr
import io
import glob
import os
import shutil

import streamlit as st

def conversion_mp3_mp4(sound_data, file_name):
    """
    sound_data : アップロードされた音声ファイル
    file_name : アップロードされた音声ファイル名
    """
    # print(os.path.splitext(file_name))
    if "mp3" in file_name:
        # print("mp3")
        sound = AudioSegment.from_file(sound_data, "mp3")
        return sound, io.BufferedRandom(sound.export(r"output.wav", format="wav"))
    elif "mp4" in file_name:
        # print("mp4")
        sound = AudioSegment.from_file(sound_data, "mp4")
        return sound, io.BufferedRandom(sound.export(r"output.wav", format="wav"))
    else:
        # print("wav")
        sound = AudioSegment.from_file(sound_data, "wav")
        return sound, io.BufferedRandom(sound.export(r"output.wav", format="wav"))

st.title("タイトル")
st.write("説明")

st.title("①ファイルから文字起こし")
st.write("説明")
file = st.file_uploader("", type="mp3")
if file:
    st.audio(file, format="audio/mp3")
    start_one = st.button("①開始")
    if start_one == True:
        conversion = conversion_mp3_mp4(file, file.name)
        # print(conversion[0])
        # print(conversion[1])
        # chunks = split_on_silence(conversion[0], min_silence_len=2000, silence_thresh=-40, keep_silence=1000)
        # print(chunks)
        r = sr.Recognizer()
        x = []
        # for i in chunks:
        #     print(i)
        with sr.AudioFile(conversion[1]) as source:

            audio = r.record(source)

        text = r.recognize_google(audio, language='ja-JP', show_all=False)
        x.append(text)
        st.write(text)

        
        contents_one = f"ファイルから文字起こしファイルから文字起こしファイルから文字起こし"
        download_one = st.download_button("①ダウンロード", contents_one)


st.title("②リアルタイムで文字起こし")
st.write("説明")
start_two = st.button("②開始")
if start_two:
    contents_two = f"リアルタイムで文字起こしリアルタイムで文字起こしリアルタイムで文字起こし"
    download_two = st.download_button("②ダウンロード", contents_two)
    st.write(f"結果表示\n\n{contents_two}")