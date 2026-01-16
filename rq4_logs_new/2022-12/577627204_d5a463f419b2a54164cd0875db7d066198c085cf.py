import pickle
import streamlit as st

# membaca model
coroner_model = pickle.load(open('jcoroner_model.sav', 'rb'))

# Judul Web
st.title('Data Mining Prediksi Jantung Koroner')

col1,col2 = st.columns(2)

with col1:
 sbp = st.text_input('Masukan Tekanan Darah Sistolik')

with col2:
 tobacco = st.text_input('Penggunaan tembakau tahunan (dalam kg)')

with col1:
 ldl = st.text_input('Masukan lipoprotein densitas rendah (Idl)')

with col2:
 adiposity = st.text_input('Tingkat adipositas (adiposty)')

with col1:
 typea = st.text_input('Masukan skor kepribadian tipe A (typea)')

with col2:
 obesity = st.text_input('Tingkat obesitas (indeks massa tubuh)')

with col1:
 alcohol = st.text_input('Tingkat penggunaan alkohol')

with col2:
 age = st.text_input('Masukan Usia')

# code untuk kelompok jenis
coroner_diagnosis = ''

# membuat tombol untuk prediksi
if st.button('Test Prediksi'):
    ane_prediction = coroner_model.predict([[sbp, tobacco, ldl, adiposity, typea, obesity, alcohol, age]])
    
    if(ane_prediction[0] == 1):
        coroner_diagnosis = 'Pasien mengidap Jantung Koroner'

    else :
        coroner_diagnosis = 'Pasien tidak mengidap Jantung Koroner'

st.success(coroner_diagnosis)