import tensorflow as tf
from transformers import TFBertForSequenceClassification
from transformers import BertTokenizer
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import numpy as np
import warnings


def load_saved_model(checkpoint_path):
    model = TFBertForSequenceClassification.from_pretrained(
        'bert-base-uncased',
        num_labels=2
    )
    loss = tf.keras.losses.SparseCategoricalCrossentropy(
        from_logits=True
    )
    metric = tf.keras.metrics.SparseCategoricalAccuracy(
        'accuracy'
    )
    optimizer = tf.keras.optimizers.Adam(
        learning_rate=2e-5,
        epsilon=1e-08
    )
    model.compile(
        loss=loss,
        optimizer=optimizer,
        metrics=[metric]
    )
    model.load_weights(checkpoint_path)
    return model

def tokenize(tweets):
    MAX_LEN = 128
    input_ids=[]
    attention_masks=[]
    for tweet in tweets:
        bert_inp=bert_tokenizer.encode_plus(
            tweet,
            add_special_tokens=True,
            max_length=MAX_LEN,
            pad_to_max_length=True,
            return_attention_mask=True
        )
        input_ids.append(bert_inp["input_ids"])
        attention_masks.append(bert_inp["attention_mask"])
    input_ids = np.array(input_ids)
    attention_masks = np.array(attention_masks)
    return input_ids, attention_masks

def tokenize_txt(txt, bert_tokenizer):
    MAX_LEN = 128
    input_ids=[]
    attention_masks=[]
    bert_inp=bert_tokenizer.encode_plus(
        txt,
        add_special_tokens=True,
        max_length=MAX_LEN,
        pad_to_max_length=True,
        return_attention_mask=True,
        truncation=True
    )
    input_ids.append(bert_inp["input_ids"])
    attention_masks.append(bert_inp["attention_mask"])
    input_ids = np.array(input_ids)
    attention_masks = np.array(attention_masks)
    return input_ids, attention_masks

bert_tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
MODEL = load_saved_model("model/BERT_WEIGHTS/bert_model_weights")

app = FastAPI()
# added
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
# \ added

class UserInput(BaseModel):
    user_input: str
        
@app.get('/')
async def index():
    return {"Message": "Prédiction de sentiment d'un texte"}

@app.post('/predict')
async def predict(UserInput: UserInput):
    txt = UserInput.user_input
    i, a = tokenize_txt(txt, bert_tokenizer)
    res = MODEL.predict(
        [
            np.array(i),
            np.array(a)
        ]
    )
    pred = np.argmax(res["logits"], axis=1)
    sentiment = ""
    if pred[0] == 0:
        sentiment = "négatif"
    if pred[0] == 1:
        sentiment = "positif"
    return {"sentiment": sentiment}

@app.get('/home')
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})