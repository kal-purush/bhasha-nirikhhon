import numpy as np
import pandas as pd
import streamlit as st
import pickle
import nltk
pickle_a=open('bagofwords.pkl','rb')
regressor=pickle.load(pickle_a)

def welcome():
    return "Welcome to my webapp"
def essay_score():
    prediction=regressor.predict()
    return prediction

def main():
    st.tilte("Automated Essay Scorer")
    html_temp="""
    <h2 style="color:red"> The essay to be written in 500 words only</h2>
    <h2> Prompt 1</h2> 
    <h3> The tight curriculum of our education system leaves no room for imagination and creativity. Write a response that expresses your thoughts on this statement. 
    To what extent do you agree or disagree? Explain your reasoning.</h3>
    <h2> Prompt 2</h2>
    <h3> Our society is disrupted by the ever-widening gap between rich and poor. One percent of the worlds population controls half of all global wealth, while a quarter of the worlds population struggles to feed themselves daily.
    Write a response describing the causes and consequences of this situation. What remedies might be effective?</h3>
    <h2> Prompt 3</h2>
    <h3>Has technology become a new addiction? Have we become slaves to our own creation? 
    Write a response that expresses your thoughts on this statement. To what extent do you agree or disagree? Explain your reasoning.</h3>
    <h2> Prompt 4</h2>
    <h3>In the nuclear age, the production and development of weaponry challenge the very existence of humankind. How useful are weapons? Do the benefits outweigh the risks? Write a response explaining the pros and cons of the arms race. Do the benefits outweigh the risks? Provide examples. Global superpowers wish to extend their influence over the entire world. Nuclear weaponry is key to this expansion.
    Write a response explaining the pros and cons of the arms race. Do the benefits outweigh the risks? Provide examples.</h3>
    """
    st.markdown(html_temp,unsafe_allow_html=True)
    prompt_number=st.text_input("Prompt number")
    essay=st.text_input("Input your essay")
    result=""
    if st.button("Predict Score"):
        result=essay_score()
    st.success("The marks out of 5 is:{}".format(result))
if __name__=="__main__":
     main()

         
