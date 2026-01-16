from flask import Flask,request,render_template,jsonify
from src.pipeline.prediction_pipeline import CustomData,PredictPipeline


application=Flask(__name__)

app=application



@app.route('/')
def home_page():
    return render_template('index.html')

@app.route('/predict',methods=['GET','POST'])

def predict_datapoint():        # this funciton will also be present in form.html
    if request.method=='GET':
        return render_template('form.html')
    
    else:
        data=CustomData(
            cap_surface = request.form.get('cap_surface'), # don't need to convert to float since catagorical data form drop down menu
            bruises= request.form.get('bruises'),
            gill_spacing = request.form.get('gill_spacing'),
            gill_size = request.form.get('gill_size'),
            gill_color = request.form.get('gill_color'),
            stalk_root = request.form.get('stalk_root'),
            stalk_surface_above_ring = request.form.get('stalk_surface_above_ring'),
            stalk_surface_below_ring = request.form.get('stalk_surface_below_ring'),
            ring_type = request.form.get('ring_type'),
            spore_print_color = request.form.get('spore_print_color'),
            population = request.form.get('population'),
            habitat = request.form.get('habitat')
        )
        final_new_data=data.get_data_as_dataframe()
        predict_pipeline=PredictPipeline()
        pred=predict_pipeline.predict(final_new_data)

        results=round(pred)

        return render_template('results.html',final_result=results)     # return the results.html to form






if __name__=="__main__":
    app.run(host='0.0.0.0',port=5000, debug=True)
