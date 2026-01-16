"""
Function untuk prediksi sentimen menggunakan LSTM
"""
import re
import pandas as pd
import pickle, re
from tensorflow.keras.preprocessing.text import Tokenizer
from keras.models import load_model
from tensorflow.keras.preprocessing.sequence import pad_sequences
import numpy as np
from cleansing import cleansing

max_features = 96
file = open('tokenizer/tokenizer.pickle','rb') # tokenizer
tokenizer = pickle.load(file)
file.close()
sentiment = ['negative', 'neutral', 'positive']

model_file_path = "model_of_lstm/model.h5"
model_file_from_lstm = load_model(model_file_path)
print('Model Loaded successfully !')
model_file_from_lstm.summary()

def lstm(text):
    text = [cleansing(text)]
    feature = tokenizer.texts_to_sequences(text)
    feature = pad_sequences(feature, maxlen=max_features)
    prediction = model_file_from_lstm.predict(feature)
    get_sentiment = sentiment[np.argmax(prediction[0])]
    return get_sentiment

def analisis_file(file_upload):
    # read csv file upload, jika eror dengan metode biasa
    df_upload = pd.DataFrame(file_upload.iloc[:,[0]])
    # rename kolom menjadi "raw_text"
    df_upload.columns = ["raw_text"]
    # bersihkan teks menggunakan fungsi cleansing
    # simpan di kolom "clean_text"
    df_upload["clean_text"] = df_upload["raw_text"].apply(cleansing)
    df_upload["sentiment"] = df_upload["clean_text"].apply(lstm)
    # mensensor kata abusive sesuai dari data dalam bentuk csv
    
    print("Cleansing text succes!")
    return df_upload