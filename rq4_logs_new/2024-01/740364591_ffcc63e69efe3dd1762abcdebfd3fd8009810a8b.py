from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
from typing import List
import pandas as pd
import io


app = FastAPI()


class Item(BaseModel):
    name: str
    year: int
    selling_price: int
    km_driven: int
    fuel: str
    seller_type: str
    transmission: str
    owner: str
    mileage: str
    engine: str
    max_power: str
    torque: str
    seats: float


class Items(BaseModel):
    objects: List[Item]


def prepare_data(data):
    data["mileage"] = data["mileage"].str.replace("kmpl", "")
    data["mileage"] = data["mileage"].str.replace("km/kg", "").astype(float)
    data["engine"] = data["engine"].str.replace("CC", "").astype(float)
    data["max_power"] = data["max_power"].str.replace("bhp", "")
    data["max_power"] = data["max_power"].str.replace(" ", "")
    data["max_power"] = pd.to_numeric(data["max_power"])
    data = data.drop(["selling_price", "torque"], axis=1)
    return data


def populate_train_data(data):
    data["mileage"].fillna(data["mileage"].median(), inplace=True)
    data["engine"].fillna(data["engine"].median(), inplace=True)
    data["max_power"].fillna(data["max_power"].median(), inplace=True)
    data["seats"].fillna(data["seats"].median(), inplace=True)
    return data


def get_clear_data_to_model(data):
    data["engine"] = data["engine"].astype(int)
    data["seats"] = data["seats"].astype(int)
    return data[["year", "km_driven", "mileage", "engine", "max_power", "seats"]]


def make_dataframe(data):
    data_for_df = (
        [item.__dict__.values() for item in data]
        if isinstance(data, List)
        else [data.__dict__.values()]
    )
    data_columns = (
        data[0].__dict__.keys() if isinstance(data, List) else data.__dict__.keys()
    )
    dataframe = pd.DataFrame(data_for_df, columns=data_columns)
    dataframe = prepare_data(dataframe)
    return get_clear_data_to_model(dataframe)


def ml_model():
    from sklearn.linear_model import Ridge  # Импортируем модель Ridge
    from sklearn.pipeline import Pipeline

    df_train = pd.read_csv(
        "https://raw.githubusercontent.com/Murcha1990/MLDS_ML_2022/main/Hometasks/HT1/cars_train.csv"
    )

    X_train = prepare_data(df_train)
    X_train = populate_train_data(X_train)
    X_train = get_clear_data_to_model(X_train)

    pipe_model = Pipeline([("model", Ridge(alpha=7.220809018385457))])
    return pipe_model.fit(X_train, df_train[["selling_price"]])


predict_model = ml_model()

@app.post("/predict_item")
def predict_item(item: Item) -> float:
    try:
        prediction = predict_model.predict(make_dataframe(item))[0]
        return prediction
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error predicting item: {str(e)}")

@app.post("/predict_items")
async def predict_items(file: UploadFile = File(...)) -> List[float]:
    try:
        df = pd.read_csv(io.StringIO(str(file.file.read(), 'utf-8')))
        items = [Item(**row) for row in df.to_dict(orient='records')]
        predictions = predict_model.predict(make_dataframe(items))
        return predictions.flatten().tolist()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error predicting items: {str(e)}")