from telnetlib import BM
from unicodedata import name
import numpy as np
import pickle
import streamlit as st
import time

loaded_model = pickle.load(open('trained_dibetes_model.sav','rb'))


#creating the function to prediction

def prediction(input_data):
    
    input_data_as_numpy_array = np.asarray(input_data)

    #reshape the array as we are predicting for one instance
    #our model is trained for 700+ elements
    input_data_reshaped = input_data_as_numpy_array.reshape(1,-1)

    prediction = loaded_model.predict(input_data_reshaped)

    if (prediction[0]==0):
        return 'Not diabetic'
    else:
        return 'Diabetic!'


def main():

    #giving the title for webpage
    st.title('Diabetes Prediction')
    st.write("Enter examined details:")

    #getting the input data from the user

    Pregnancies = st.text_input('Number of Pregnancies')
    Glucose = st.text_input('Blood-Glucose level')       
    BloodPressure = st.text_input('Blood Pressure level')    
    SkinThickness = st.text_input('Skin Thickness value')   
    Insulin = st.text_input('Insulin level')   
    BMI = st.text_input('BMI value')   
    DiabetesPedigreeFunction = st.text_input('Diabetes Pedigree Function value')   
    Age = st.text_input('Age of the Person')   

    #code for prediction
    diagnosis = ''

    #creating a button for prediction
    if st.button('Test'):
        diagnosis = prediction([Pregnancies,Glucose,BloodPressure,SkinThickness,
        Insulin,BMI,DiabetesPedigreeFunction,Age])

    # st.warning(diagnosis)
    # st.balloons()
        st.balloons()

    if(diagnosis!="Diabetic!"):
        st.info(diagnosis)
    else:
        st.error(diagnosis)
        



if __name__ == '__main__':
    main()

#open anaconda
# go to  environments and run with terminal
#type  streamlit run "path for this file with same backslash"