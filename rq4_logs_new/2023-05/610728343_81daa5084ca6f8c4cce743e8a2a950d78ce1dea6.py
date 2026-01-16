from flask import Flask, render_template,request
import pandas as pd
import numpy as np
import pickle

app = Flask(__name__)

@app.route('/')
def home():
    data = pd.read_csv("employee_promotion.csv")
    values = {}
    values['department_values'] = sorted(data["department"].unique())
    values['region_values'] = sorted(data["region"].unique(), key=lambda region: int(region.split('_')[1]))
    values['education_values'] = sorted(data["education"].dropna().unique())
    values['recruitment_values'] = sorted(data['recruitment_channel'].unique())
    return render_template('home.html', **values)

@app.route("/promotion-status", methods = ['POST'])
def promotion_status():
    department = request.form.get('department')
    region = request.form.get('region')
    education = request.form.get('education')
    recruitment_channel = request.form.get('recruitment_channel')
    gender = request.form.get('gender')
    age = request.form.get('age')
    no_of_trainings = request.form.get('no_of_trainings')
    length_of_service = request.form.get('length_of_service')
    previous_year_rating = request.form.get('previous_year_rating')
    avg_training_score = request.form.get('avg_training_score')
    awards_won = request.form.get('awards_won')

    emp_details = [department, region, education, gender, recruitment_channel, no_of_trainings, age, previous_year_rating, length_of_service, awards_won, avg_training_score]
    col_trans = pickle.load(open('transformer.pkl', 'rb'))
    fs = pickle.load(open('feature_select.pkl', 'rb'))
    model = pickle.load(open('model.pkl', 'rb'))
    
    emp_details = pd.DataFrame([emp_details], columns=pd.read_csv('employee_promotion.csv').drop(['employee_id', 'is_promoted'], axis=1).columns)
    X_test = col_trans.transform(emp_details)
    X_test = pd.DataFrame(X_test, columns=col_trans.get_feature_names_out())
    X_test = fs.transform(X_test)
    promotion_status = model.predict(X_test)

    if promotion_status[0]:
        out = "Congrats, you have a high chance of getting promoted"
    else:
        out = "You have a low chance of getting promoted"
    return render_template('promotion_status.html', promotion_status = out)