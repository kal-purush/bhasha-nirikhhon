from flask import Flask, render_template
app = Flask(__name__,static_url_path="/static")
from flask import jsonify
import os.path
import sys
import json
import pandas as pd
from flask import request
import requests


@app.route("/")
def template_test():
    print("hello")
    return render_template('initial.html')


@app.route("/rest/api/")
def api_call():
    q = request.args.get('q')
    print(q)
    value = fun(q)
    #value = json.loads(value)
    return (value)

def fun(q):
    choice = q
    print(choice)
    #choice = "ALBDULA000048805"
    ##Packages
    import pandas as pd
    import os
    import csv
    #os.chdir("E:\\PoCs\\SE2")  ## have some library
    
    #Read OIPA
    OIPA_data = pd.read_csv("OIPA.csv")
    
    #Filter the required data
    test_rec = OIPA_data.loc[(OIPA_data['Client_First_Name'] == choice ) | (OIPA_data['Client_Last_Name'] == choice) | (OIPA_data['Policy_Number'] == choice)  ] 
    
    
    #Rule based model
    def Classify(test_rec):
        if test_rec.Attained_age_at_Issue.values < 50 and test_rec.Marital_Status.values == 'single' and test_rec.Income_per_Annum.values < 80000 and test_rec.Credit_Score.values > 300 and test_rec.Employment_Status.values == 'Working' and test_rec.Risk_Appetite_Score.values > 5 and test_rec.SnP_500_Index_Score.values > 6 :
            #print("Financial Guidance = 'Aggressive'")
            return("Aggressive")
        if test_rec.Attained_age_at_Issue.values >= 50 and test_rec.Marital_Status.values == 'Married' and test_rec.Income_per_Annum.values > 80000 and test_rec.Credit_Score.values > 400 and test_rec.Employment_Status.values == 'Working' and test_rec.Risk_Appetite_Score.values <= 5 and test_rec.SnP_500_Index_Score.values < 6 :        
            #print("Financial Guidance ='Safetynet'")
            return("Safetynet")
        
    category = Classify(test_rec)
    
    # Loading the fund values
    
    
    csv_file = csv.reader(open('Current_Fund_Values1.csv'), delimiter=",")
    for row in csv_file:
        print(row)
        if choice == row[0]:
            print("in")
            #Policy_Status = row[3]
            #Contract_Issue_date = row[4]
            #Total_Net_Worth = row[5]
            input1 = row[6]
            print(input1)
            input2 = row[7]
            print(input2)
            input3 = row[8]
            print(input3)
            input4 = row[9]
            print(input4)
            input5 = row[10]
            print(input5)
    
    Current_Investment_Cashvalue_equity = float(input1)
    Current_Investment_Cashvalue_MM = float(input2)
    Current_Investment_units_equity = float(input3)        
    Current_Investment_units_MM = float(input4)
    Todays_Unit_Value = float(input5)
    
    ## Recommendatiion calculation
    
    if category == "aggressive":
        total = Current_Investment_Cashvalue_equity + Current_Investment_Cashvalue_MM
        equity_share = (Current_Investment_Cashvalue_equity / total)*100
        money_market_share = (Current_Investment_Cashvalue_MM / total)*100
        predict_equity_percent = 90.0000
        predict_mm_percent = 10.0000
        difference_equity = predict_equity_percent - equity_share
        difference_mm = predict_mm_percent - money_market_share
        Recommeneded_Equity_Investment = total * 0.9
        Recommended_MM_Investment = total * 0.1
        
    
    elif category == "Safetynet":
        total = Current_Investment_Cashvalue_equity + Current_Investment_Cashvalue_MM
        equity_share = (Current_Investment_Cashvalue_equity / total)*100
        money_market_share = (Current_Investment_Cashvalue_MM / total)*100
        predict_equity_percent = 40.0000
        predict_mm_percent = 60.0000
        difference_equity = predict_equity_percent - equity_share
        difference_mm = predict_mm_percent - money_market_share
        Recommeneded_Equity_Investment = total * 0.4
        Recommended_MM_Investment = total * 0.6
        
    ## output values that needs to be passed via API
    
    Equity_fund_name = "Alamere Equity Income"
    MM_fund_name = "Alamere Money Market"
    
    #Before Fund Switch
    Before_Fund_Switch_Equity_Cash_Value = Current_Investment_Cashvalue_equity
    Before_Fund_Switch_Equity_Outstanding_units = Current_Investment_units_equity
    Before_Fund_Switch_Equity_Percent_Split = (equity_share * 100)
    Before_Fund_Switch_MM_Cash_Value = Current_Investment_Cashvalue_MM
    Before_Fund_Switch_MM_Outstanding_units = Current_Investment_units_MM
    Before_Fund_Switch_MM_Percent_Split = (money_market_share * 100)
    
    #Recommended Split
    Recommended_Equity_Split = predict_equity_percent
    Recommended_MM_Split = predict_mm_percent
    
    
    #After Fund Switch
    After_Fund_Switch_Equity_Cash_Value = Recommeneded_Equity_Investment
    After_Fund_Switch_Equity_Units = After_Fund_Switch_Equity_Cash_Value / 10.8774121
    After_Fund_Switch_MM_Cash_Value = Recommended_MM_Investment
    After_Fund_Switch_MM_Units = After_Fund_Switch_MM_Cash_Value / 10.8774121

    val = {"BeforeCashVal":Before_Fund_Switch_Equity_Cash_Value,"BeforeOutstanding":Before_Fund_Switch_Equity_Outstanding_units,"BeforePercentSplit":Before_Fund_Switch_Equity_Percent_Split,"BeforeMMCashVal":Before_Fund_Switch_MM_Cash_Value,"BeforeMMOutstanding":Before_Fund_Switch_MM_Outstanding_units,"BeforeMMPercentSplit":Before_Fund_Switch_MM_Percent_Split,"RecommendedEquitySplit":Recommended_Equity_Split,"RecommendedMM":Recommended_MM_Split,"AfterCashValue":After_Fund_Switch_Equity_Cash_Value,"AfterEquityUnits":After_Fund_Switch_Equity_Units,"AfterMMCashValue":After_Fund_Switch_MM_Cash_Value,"AfterMMEquityUnits":After_Fund_Switch_MM_Units}
    return (json.dumps(val))
	
if __name__ == '__main__':
    #app.run(ssl_context='adhoc')
    app.run(host='0.0.0.0', port=7000)