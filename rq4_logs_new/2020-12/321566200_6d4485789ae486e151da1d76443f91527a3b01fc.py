from flask import Flask, render_template, request,jsonify
import numpy as np
#import requests
import pickle
import preprocessing as prep
from scipy.sparse import hstack, csr_matrix

app = Flask(__name__)

#model = joblib.load(open('./model/rf_model.pkl', 'rb'))
model = pickle.load(open('./model/xgb_hyperpara_model_3.pkl', 'rb'))
x_vector = pickle.load(open('./model/x_vector_transform.pkl', 'rb'))
                         
@app.route('/',methods=['GET'])
def Home():
    return render_template('index.html')



@app.route("/predictSpam", methods=['POST'])
def predictSpam():

    if request.method == 'POST':
        text = ''
        text = str(request.form['txtmessage'])
        if text:
            df = prep.create_dataframe(text)
            df.drop(['text'], inplace=True, axis=1)
            
            #input_vectors = vec.create_vectors(df)
            
            x_vectors = x_vector.transform(df['cleaned_text'].apply(lambda x: np.str_(x)))
            
            #combine features
            selected_features = df.columns[1:]
            feature_set1 = df[selected_features]

            #converting panda frame=feature_set1 to compress sparse notatation 
            input_vectors = hstack([x_vectors, csr_matrix(feature_set1)], "csr")
            
            #vectors = X_vector.fit_transform(df).toarrary()
            prediction = model.predict(input_vectors)
            output=round(prediction[0])
        
        if not text:
            return render_template('index.html',prediction_text="Sorry, input is required for analysis!")
        
        else:
            '''
            if output == 0:
                return jsonify({output:"This is legit message"})
            else:
                return jsonify({output:"This is Spam message"})
            '''
            if output == 0:
                return render_template('index.html',prediction_text="This is legit message! Model Predicted Value =  " + str(output))
            else:
                return render_template('index.html', prediction_text="This is spam. Be careful! Model Predicted Value = " + str(output))
            
    else:
        return render_template('index.html')

if __name__=="__main__":
    app.run(debug=True)
