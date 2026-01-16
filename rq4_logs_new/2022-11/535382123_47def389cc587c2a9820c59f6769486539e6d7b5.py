import bentoml
from bentoml.io import JSON
from bentoml.io import NumpyNdarray
from numpy import expm1


model_ref = bentoml.xgboost.get("fire_loss_model:latest")
dv = model_ref.custom_objects['dictVectorizer']

model_runner = model_ref.to_runner()

svc = bentoml.Service("fire_loss_model", runners=[model_runner])


@svc.api(input=JSON(), output=JSON())
def predict(app_data):
    
    vector = dv.transform(app_data)
    prediction = model_runner.predict.run(vector)
    
    print(prediction)
    
    result = float(expm1(prediction)[0])

    print(result)

    return {"estimated dollar loss" : round(result, 2)}