# 1. Library imports
import pandas as pd
import pickle
from os import path as op
import fastapi
import uvicorn ## ASGI
import lightgbm as lgb
import shap

# 2. Create app
app = fastapi.FastAPI()
userid: int

# 3. Import dataset
df = pd.read_csv(r'C:\Users\khale\Desktop\projet 7\data.csv.csv', low_memory=False)

# 4. Make a prediction based on the user-entered id
# Returns the predicted class with its respective probability
@app.post('/predict')
def predict_score(userid):
    int_id = int(userid)
    # load the model from disk
    model = pickle.load(open(op.join(r'C:\Users\khale\Desktop\projet 7\light_gbm_f2.sav'), 'rb'))
    data_in = df[df['identifiant'] == int_id]
    del (data_in['identifiant'])
    prediction = model.predict(data_in)
    probability = model.predict_proba(data_in).max()
    # Explainability
    explainer = shap.TreeExplainer(model)
    shap_value = explainer.shap_values(data_in)
    pos = [108, 109, 130, 126, 66]
    colname = df.columns[pos]
    important_features = colname
    if probability > 0.6:
        print('Your loan is accepted')
    elif probability < 0.6:
        print('Your loan is not accepted')

    return {
            'prediction': prediction[0],
            'probability': probability,
            'explainer': explainer.expected_value[1],
            'shap_val': shap_value[1][0].tolist(),
            'col': important_features.tolist()
            }


# 4. Run the API with uvicorn
#    Will run on http://127.0.0.1:8000
if __name__ == '__main__':
    uvicorn.run(app, host='127.0.0.1', port=8000)