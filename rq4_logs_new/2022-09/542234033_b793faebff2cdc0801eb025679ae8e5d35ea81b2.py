import streamlit as st
import pickle

# load the model from disk
model_filename = 'meter nombre del archivo pickle generado.pkl'
loaded_model = pickle.load(open(model_filename, 'rb'))
# load the scaler from disk
scaler_filename = 'meter nombre del archivo pickle scaler generado.pkl'
loaded_scaler = pickle.load(open(scaler_filename, 'rb'))

st.title("Palladium")
st.write("""
         Modelo de predicción de cancelaciones de reservas según datos de Palladium
         """)

NOCHES = st.text_input("Número de noches")
HOTEL = st.selectbox("ID_HOTEL", options=['NOMBRE HOTEL 1', 'NOMBRE HOTEL 2'...ETC.])
MES = st.selectbox("MES LLEGADA", options=["ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO", "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"])
DÍA_SEMANA = st.selectbox("WEEKDAY_LLEGADA", options=["LUNES", "MARTES", "MIÉRCOLES", "JUEVES", "VIERNES", "SÁBADO", "DOMINGO"])
SEMANA_DEL_AÑO = st.slider("WEEK_LLEGADA", 1, 48, 1)


age = st.slider("Edad", 1, 100, 1)
p_class = st.selectbox("Clase", options=['Primera clase', 'Segunda clase', 'Tercera clase'])

sex = 0 if sex == 'Hombre' else 1

f_class = 1 if p_class == 'Primera clase' else 0
s_class = 1 if p_class == 'Segunda clase' else 0
t_class = 1 if p_class == 'Tercera clase' else 0

input_data = loaded_scaler.transform([[sex, age, f_class, s_class, t_class]])
prediction = loaded_model.predict(input_data)
predict_probability = loaded_model.predict_proba(input_data)

if name != "":
    if prediction[0] == 1:
        st.write(f":+1: El pasajero {name} **SI** hubiera sobrevivido con una probabilidad \
        de {round(predict_probability[0][1] * 100, 3)}%")
    else:
        st.write(f":cry: El pasajero {name} **NO** hubiera sobrevivido con una probabilidad \
        de {round(predict_probability[0][0] * 100, 3)}%")