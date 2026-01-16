import pandas as pd
from flask import Flask, request, render_template
import joblib

# Create Flask app
app = Flask(__name__)

# Load the trained model
model = joblib.load('ev_temp_regression.pkl')

# Define the home route
@app.route("/")
def home():
    return render_template("index.html")

# Define the prediction route
@app.route('/predict', methods=["POST"])
def predict():
    if request.method == "POST":
        # Get input values from the HTML form
        profile_id = float(request.form['profile_id'])
        u_q	= float(request.form['u_q'])
        coolant = float(request.form['coolant'])
        u_d = float(request.form['u_d'])
        motor_speed = float(request.form['motor_speed'])
        i_d = float(request.form['i_d'])
        i_q = float(request.form['i_q'])
        ambient = float(request.form['ambient'])

        # Create a DataFrame with the input data
        data = pd.DataFrame({
            'profile_id': [profile_id],
            'u_q': [u_q],
            'coolant': [coolant],
            'u_d': [u_d],
            'motor_speed': [motor_speed],
            'i_d': [i_d],
            'i_q': [i_q],
            'ambient': [ambient]})
        
        # Make prediction using the loaded model
        prediction = model.predict(data)

        return render_template("index.html", prediction_text=prediction)
    
# Run the app if executed directly
if __name__ == "__main__":
    app.run()
    app.run(host="0.0.0.0", port=80)