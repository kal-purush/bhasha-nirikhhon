from fastapi import FastAPI, File, UploadFile
from typing import List, Tuple
import shutil
import cv2
import numpy as np
import glob
import random

import yolo_object_detection
from fastapi.middleware.cors import CORSMiddleware

import base64
app = FastAPI()

origins = [
    "http://localhost:3000",
    "localhost:3000"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.get("/", tags=["root"])
async def read_root() -> dict:
    return {"message": "Welcome to object detection project  "}

## Detect pile human veiw
@app.post("/image/humanVeiw")
async def image(image: List[UploadFile]= File(...)):
    # print(image)
    allResult = []
    for image in image:
        # print(image.filename)
        with open(f"./write_humanVeiw_Img/{image.filename}", "wb") as buffer:
            result = shutil.copyfileobj(image.file, buffer)
            # print(image)
            pile = yolo_object_detection.predict_humanView(image.filename,allResult)

    return  pile


## Detect pile drone veiw
@app.post("/image/droneVeiw")
async def image(image: List[UploadFile]= File(...)):
    # print(image)
    allResult = []
    for image in image:
        # print(image.filename)
        with open(f"./write_droneVeiw_Img/{image.filename}", "wb") as buffer:
            result = shutil.copyfileobj(image.file, buffer)
            # print(image)
            pile = yolo_object_detection.predict_droneVeiw(image.filename,allResult)

    return  pile