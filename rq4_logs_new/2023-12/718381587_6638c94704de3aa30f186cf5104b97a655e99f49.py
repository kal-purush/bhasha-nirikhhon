# app.py
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import mlflow
import optuna
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import io
import base64
from src.support_analize import *
from src.database import *
from src.preprocessing import *
from scripts.training import *
from scripts.evaluation import *

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
async def read_root():
    df=get_data()
    df=add_data(df)
    df=data_clustering(df)
    run_optimization(df)
    return {"message": "Clustering optimization completed. Check MLflow UI for details."}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)