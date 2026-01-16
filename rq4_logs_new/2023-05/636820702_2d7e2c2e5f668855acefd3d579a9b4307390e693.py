# Data Handling
import logging
import pickle
import numpy as np
import pandas as pd
from pydantic import BaseModel

# Server
import uvicorn
import gunicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

#Classifier
from xgboost import XGBClassifier

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize logging
my_logger = logging.getLogger()
my_logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.INFO, filename='sample.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

# Initialize files
xgb_model = XGBClassifier()
xgb_model.load_model('data/xgb_model_2021_09_13_1042.pkl')
nb_model = pickle.load(open('data/nb_model_2021_09_13_1042.pkl','rb'))
lr_model = pickle.load(open('data/lr_model_2021_09_13_1042.pkl','rb'))
encoder = pickle.load(open('data/ord_enc_2021_09_13_1042.pkl','rb'))
scaler = pickle.load(open('data/std_scaler_2021_09_13_1042.pkl','rb'))
features = pickle.load(open('data/features_2021_09_13_1042.pkl','rb'))
numerical_features = pickle.load(open('data/numerical_features_to_scale_2021_09_13_1042.pkl','rb'))
categ_features = pickle.load(open('data/categ_features_2021_09_13_1042.pkl','rb'))

class Data(BaseModel):
    campus:str
    curso:str
    modalidade: str 
    genero:str
    raca:str
    idioma:str
    ficou_tempo_sem_estudar:str
    companhia_domiciliar:str
    mae_nivel_escolaridade:str
    pai_nivel_escolaridade:str
    estado_civil:str
    tipo_area_residencial:str
    trabalha:str
    rendabruta:float
    ira: float
    idade:int
    anoingresso:int
    periodoingresso:int
    quantidade_computadores:int
    quantidade_notebooks:int
    qtd_filhos:int
    reprovacoes:int
    reprovacoes_por_falta:int
    percentual_carga_horaria_frequentada_mean:float
    carga_horaria_total:int
        
def transform(data: Data):
    # Extract data to dataframe in the same order of trainning
    X_test = pd.DataFrame(data.dict(),index=[0])
    X_test = X_test[features]

    # Apply encoding and scaling
    X_test[categ_features] = encoder.transform(X=X_test[categ_features])
    X_test[numerical_features] = scaler.transform(X=X_test[numerical_features])
    
    return X_test
        
@app.get("/")
async def verify_online_api():
    return "API ONLINE"
    
@app.post("/predict")
async def predict(data: Data):
    try:
        X_test = transform(data)
        
        # Create and return prediction
        y_pred = xgb_model.predict(X_test)
        return {"prediction": int(y_pred[0])}
    except:
        my_logger.error("Something went wrong!")
        return {"prediction": "error"}