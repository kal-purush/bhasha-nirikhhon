from flask import Flask, render_template, request, jsonify,flash,redirect,url_for,jsonify,make_response


from flask_restful import Resource,Api

import pandas as pd
from roadconditionclass import prepare_modelinput
import pickle
import mplleaflet
import os

import json
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import numpy as np
from flask import send_file

app = Flask(__name__,static_url_path='/static')
sumrapi = Api(app)

app.config['SECRET_KEY'] = 'b58f1ad3ab27a913c64246143682ebf5'

@app.route('/')
def index():
    file = open('var.txt','r')
    var = json.loads(file.read())

    values = {1.0: 4636, 2.0: 436, 0.0: 322}


    piechart = json.dumps([{
        "label": "Clean  Road",
        "value": str(values[1.0])
    }, {
        "label": "Bad Road",
        "value": str(values[0.0])
    }, {
        "label": "VeryBad Road",
        "value": str(values[2.0])
    }])

    return render_template('index.html',var=json.dumps(var),piechart=piechart,values=values)


@app.route('/steps')
def step():


    return render_template('Steps.html')

@app.route('/Download/<path:filename>')
def downloadFile (filename):
    print(filename,"="*100)
    path = os.path.join(os.getcwd(),filename)
    return send_file(path, as_attachment=True)



@app.route('/view', methods=['POST','GET'])
def view():

		
    try:

        gps = request.files['gps']
        gyroscope = request.files['gyroscope']
        if (gps.content_type != 'text/csv') & (gyroscope.content_type != 'text/csv'):

            flash('Please upload csv file')
            return redirect(url_for('index'))

        gps = pd.read_csv(gps)
        gyro = pd.read_csv(gyroscope)

        data = prepare_modelinput(timestep=10,gps=gps,gyroscope=gyro,model_path=str(os.path.join(os.getcwd(),'model.p')))


        output = data.clean_adujst_accuracy(20)

        fig,ax = plt.subplots(figsize=(12,9))
        ax.plot(data.longitude,data.lattitude,c='green')
        ax.scatter(output[output['CleanCondition'] == 0]['Lon'].values.tolist(),output[output['CleanCondition'] == 0]['Lat'].values.tolist(),c='blue',alpha=1)
        ax.scatter(output[output['CleanCondition'] == 2]['Lon'].values.tolist(),output[output['CleanCondition'] == 2]['Lat'].values.tolist(),c='red')
        var = json.dumps(mplleaflet.fig_to_geojson(fig=fig))
        
        values = output['CleanCondition'].value_counts().to_dict()

        piechart = json.dumps([{
            "label": "Clean  Road",
            "value": str(values[1.0])
        }, {
            "label": "Bad Road",
            "value": str(values[0.0])
        }, {
            "label": "VeryBad Road",
            "value": str(values[2.0])
        }])


    except :
        flash('please upload file')
        return redirect(url_for('index'))


    return render_template('search.html',var=var,piechart= piechart,values=values)



def make_prediction(numpy1):
    model = pickle.load( open( "model.p", "rb" ))
    prediction_out = model.predict(numpy1)
    return model.predict_classes(numpy1)

class Linksumr(Resource):
    def get(self):
        return {"numpyarray":"Enter your array in shape (sample,timestep,feature) i.e (40,10,3"}
    def post(self):
        data = request.get_json()
  
        inputnumpy = data['numpyarray']
        numpy1 = np.array(inputnumpy)


        p = make_prediction(numpy1)


        return {'Output':str(p)}


sumrapi.add_resource(Linksumr,'/predict')
 
if __name__ == '__main__':
    

    
    app.run(host='0.0.0.0',debug = True)




'''
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"numpyarray":[[[0.01732835, 0.8245989 , 0.0720054 ],[0.61879624, 0.81427569, 0.66685889],
        [0.6466509 , 0.66167216, 0.14050801],
        [0.72499171, 0.03914283, 0.54737222],
        [0.20848813, 0.12985419, 0.14895229],
        [0.68687396, 0.85619549, 0.57348964],
        [0.06813701, 0.56330812, 0.41182815],
        [0.37490412, 0.24444778, 0.70499129],
        [0.70121111, 0.2147882 , 0.94352369],
        [0.8823995 , 0.89301293, 0.85990209]]]}' \
        https://roadcondition.herokuapp.com/predict
  '''


