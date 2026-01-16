import streamlit as st
import os, sys, pickle
import importlib.util
import pandas as pd

# Parse command-line argument (folder)
if len(sys.argv) > 1:
    folder = os.path.abspath(sys.argv[1])
else:
    folder = os.path.abspath(os.getcwd())


# Get filenames for all python files in this path, excluding this script
thisFile = os.path.abspath(__file__)
fileNames = []
for baseName in os.listdir(folder):
	fileName = os.path.join(folder, baseName)
	if (fileName.endswith(".py")) and (fileName != thisFile):
		fileNames.append(fileName)

# Filename formatter to display a nicer url (instead of the whole github path)
def format_url(s):
	els = s.split("/")[-1].split(".")[0].split("_")
	return " ".join(el for el in els).capitalize()

# 
st.title('Cover Judger')

# Function to load model
@st.cache # cache
def load_model(modelName):
	model = pd.read_pickle(os.path.join(folder, 'models', modelName + '.sav'))
	return model

# Load Model
modelName = 'model_collab_KNNWithMeans'
model = load_model(modelName)

# 
st.subheader('Inputs')
st.write('Your Goodreads ID can be found in your URL when on your profile page')
#st.image(url_to_image, format='PNG')
user_id = st.text_input('Goodreads Numeric User ID:', '8842281e1d1347389f2ab93d60773d4d')

st.write('The Goodreads book ID can be found in the book URL')
book_id = st.text_input('Goodreads Numeric Book ID:', '24375664')

st.subheader('Estimated Rating(s)')

# Make Predictions
if (user_id is not None) & (book_id is not None):
	# Make Prediction
	pred = model.predict(book_id, user_id, r_ui=None, verbose=False)
	# Convert to Dataframe
	pred_col_names = ['book_id', 'user_id', 'true_rating', 'est_rating', 'details']
	pred_df = pd.DataFrame(columns=pred_col_names)
	pred_sr = pd.Series(pred, index=pred_col_names)
	pred_df = pred_df.append(pred_sr, ignore_index=True)
	st.write(pred_df['est_rating'])