from flask import Flask, render_template, request
import pickle
from sklearn.preprocessing import StandardScaler

app = Flask(__name__)
model = pickle.load(open('fitness.pkl', 'rb'))

@app.route('/',methods=['GET'])
def Home():
    return render_template('index.html')


standard_to = StandardScaler()

@app.route("/predict", methods=['POST'])
def predict():
    if request.method == 'POST':
        Height = int(request.form['Height'])
        Weight=int(request.form['Weight'])

        Gender_Male=request.form['Gender']
        if(Gender_Male=='Male'):
            Gender_Male=1
        else:
            Gender_Male=0
        val = [(Weight,Height,Gender_Male)]
        prediction = model.predict(val)

        return render_template('index.html', prediction_text="Yau are {}".format(prediction))

    else:
        return render_template('index.html')

if __name__=="__main__":
    app.run(debug=True)
