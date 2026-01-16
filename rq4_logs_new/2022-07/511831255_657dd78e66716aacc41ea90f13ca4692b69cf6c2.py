import joblib
import pandas as pd

timeline = 26 
blue_destr_tower = 1
red_destr_tower = 0
blue_gold = 32100
red_gold = 21800
kill_1 = 4
death_1 = 3 
assist_1 = 4
kill_2 = 5
death_2 = 0
assist_2 = 7
kill_3 = 3
death_3 = 0
assist_3 = 3
kill_4 = 6
death_4 = 1
assist_4 = 2
kill_5 = 4
death_5 = 1
assist_5 = 3
kill_6 = 1
death_6 = 4
assist_6 = 2
kill_7 = 0
death_7 = 5
assist_7 = 2
kill_8 = 4
death_8 = 1
assist_8 = 0
kill_9 = 0
death_9 = 8
assist_9 = 1
kill_10 = 0
death_10 = 4
assist_10 = 2

list_of_variable = [int(timeline), int(blue_destr_tower), int(red_destr_tower), int(blue_gold), int(red_gold), int(kill_1), int(death_1), int(assist_1), int(kill_2), int(death_2), int(assist_2), int(kill_3), int(death_3), int(assist_3), int(kill_4), int(death_4), int(assist_4), int(kill_5), int(death_5), int(assist_5),
int(kill_6), int(death_6), int(assist_6), int(kill_7), int(death_7), int(assist_7), int(kill_8), int(death_8), int(assist_8), int(kill_9), int(death_9), int(assist_9), int(kill_10), int(death_10), int(assist_10)]
filename = "/home/apprenant/simplon_project/league_of_predicts/notebook/regression_logistique"
classifier = joblib.load(open(filename, 'rb'))
predictions = classifier.predict([list_of_variable])
predictions_proba = classifier.predict_proba([list_of_variable])

print(predictions)
print(predictions_proba)

labels = ["Lose","Win"]
data = {
    'Blue Win' : {
    'Probabilities' : predictions_proba[0][1]},
    'Blue lose' : {
    'Probabilities' : predictions_proba[0][0]},
    'Prediction' : labels[predictions[0]]
       }