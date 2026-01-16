import streamlit as st
import pandas as pd
import random   # not necessary to import, but we will use it to generate a random number

st.title('☆Rat Fan Club☆')  # Set the title of the app

st.write('### The cheese formula:')  # display some text
st.latex(r'\left( i\gamma^\mu\partial_\mu - m \right) \psi = 0')    # display a LaTeX formula
st.write('Just kidding, that is the **Dirac equation**. To be honest, I do not understand it at all.')

# display an image
st.image('data/streamlit/rat.jpeg', caption='This is the rat in question', width=300)

st.write('### ⭐🔮 The Fortune Telling Rat 🔮⭐')

# read some data (the fortunes) from a file
with open('data/streamlit/fortune.txt', 'r') as f:
    fortunes = f.readlines()

# add a text box for the user to enter their name
name = st.text_input('**Enter your name** before the rat can reveal your fortune:', '')    # get the user's name

# add a button
if st.button('**I want to know**'): # this executes when the button is clicked

    if not name:    # if the user did not enter a name, display a warning message
        st.write(':red[Enter name to get your fortune]')
        st.stop()   # stop the execution of the script

    # display a random fortune for the user
    random_index = random.randint(0, len(fortunes) - 1) # generate a random number
    with st.empty():
        st.write(f'*☆ {fortunes[random_index]} ☆*')    # display the fortune
