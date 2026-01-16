from flask import Flask, render_template, request
import pickle
from prophet.plot import plot_plotly
import pandas as pd

app = Flask(__name__)
def load_and_predict_model(model_path, date, period=365*2):
    with open(model_path, 'rb') as f:
        model = pickle.load(f)

    future = model.make_future_dataframe(periods=period)
    predictions = model.predict(future)
    selected_date = pd.to_datetime(date)
    selected_prediction = predictions[predictions['ds'] == selected_date]

    rounded_predictions = []
    for prediction in selected_prediction[["yhat_lower", "yhat", "yhat_upper"]].values.tolist():
        rounded_values = [round(value, 2) for value in prediction]
        rounded_predictions.append(rounded_values)

    plot_div = plot_plotly(model, predictions).to_html()
    
    return date, rounded_predictions, plot_div

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/predict', methods=['POST']) # type: ignore
def predict():
    date = request.form.get('date')
    option = request.form.get('Option')

    if option == "1":
        date, predictions, plot_div = load_and_predict_model('Model/prophet_model_meantemp.pkl', date)
        return render_template('result.html', date=date, predicted_temp=predictions, plotDiv=plot_div, don_vi="Độ C")
    elif option == "2":
        date, predictions, plot_div = load_and_predict_model('Model/prophet_model_humidity.pkl', date)
        return render_template('result.html', date=date, predicted_temp=predictions, plotDiv=plot_div, don_vi="%")
    elif option == "3":
        date, predictions, plot_div = load_and_predict_model('Model/prophet_model_windspeed.pkl', date)
        return render_template('result.html', date=date, predicted_temp=predictions, plotDiv=plot_div, don_vi="Km/h")
    elif option == "4":
        date, predictions, plot_div = load_and_predict_model('Model/prophet_model_meanpressure.pkl', date)
        return render_template('result.html', date=date, predicted_temp=predictions, plotDiv=plot_div, don_vi="atm")
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)