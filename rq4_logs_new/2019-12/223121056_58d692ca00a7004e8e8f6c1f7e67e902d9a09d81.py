# INSERT YOUR VARIABLES ------------------------------------------------------

project_name="PROJEKTNAME"

input_directory = "C:/Users/Florian Gehrig/Box/02. Live Engagements/BayWa r.e. - Internal Engagement (2019)/03-Rollout/01-Rework Strategic Directives/99-Employee Feedback Survey/00-Data/2019-08-18-Rohdaten von Umfrage XXX._employee_survey (alle Teilnehmer)_WHOLE_FINAL_v6.csv"
output_directory = "C:/Users/Florian Gehrig/Documents"

variables_of_interest = ["Q7 - 1"]
groupings=["Q3"]
cols=["Q6"]
n_cols=[10]
font_directory = "C:/Users/Florian Gehrig/Downloads/arialbd.ttf"

translation = False
target_language = "en" # "en" for English, "de" for German...
translation_export = False


###############################################################################

# 1. LIBRARY LOADING ----------------------------------------------------------

# 1.1 DATA MANIPULATION ---

import pandas as pd
import numpy as np
import os
import sys
sys.path.insert(0, (os.path.expanduser(os.getenv('USERPROFILE')).replace('\\','/') +'/Documents/GitHub/Analytics-Python/Text Analytics'))


# 1.2  TEXT ANALYTICS ---

from nltk.corpus import stopwords
from textanalytics import *


# 2. SETTINGS -----------------------------------------------------------------

os.chdir(output_directory)
pd.set_option('display.max_columns', 1000)
np.set_printoptions(threshold=sys.maxsize)

stop_words = set(stopwords.words('english'))

def byw_grey(word, *args, **kwargs):
    return "#4c4c4c"


# 3. DATA LOADING -------------------------------------------------------------

df = pd.read_csv(input_directory, encoding = "ISO-8859-1")

if translation == True:
    df, translates = translating(key_directory, df, target_language)

    if translation_export == True:
        df_translated.to_csv(output_directory+"/"+project_name+"_translated data.csv")


# CROSSTABS -------------------------------------------------------------------

theme_crosstabs(df, cols, n_cols, project_name, groupings)


# THEME CO-OCCURENCE MATRIX ---------------------------------------------------

theme_correlation(df,project_name,cols, n_cols)


# TOPIC-BASED WORDCLOUD CREATION ----------------------------------------------

theme_based_wordcloud(df,cols,n_cols,groupings,font_directory)


# GROUPING-BASED WORDCLOUD CREATION --------------------------------------------

group_based_wordcloud(df, cols, n_cols, groupings, font_directory, color_function=byw_grey(), scaling=1)


# ASSOCIATION MAP CREATION  ---------------------------------------------------
# (WARING! WORKS ONLY FOR ENGLISH INPUT)

output = key_term_plot(df,variables_of_interest, dims=[3],scaling=1.5, as_interactive = True)
