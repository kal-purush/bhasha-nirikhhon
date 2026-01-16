# Diego Souza Lima Marques - TIA: 32039921
# Lucas de Camargo Gonçalves - TIA: 32074964
# Laboratório 06 - Webservices/Balanceamento de carga

from flask import Flask
from flask import jsonify
from flask import request
import urllib.request, json

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

@app.route('/convertemoeda/<int:VALOR>',methods=['GET'])
def getConversion(VALOR):
    url = "https://economia.awesomeapi.com.br/json/last/BRL-USD,BRL-EUR"
    response = urllib.request.urlopen(url)
    data = response.read()
    jsonf = json.loads(data)
    
    real = VALOR
    dolar = jsonf["BRLUSD"]["high"]
    euro = jsonf["BRLEUR"]["high"]

    resposta = {
    "conversao":
        {
            "real":real,
            "dolar":round((float(dolar)*real),3),
            "euro":round((float(euro)*real),3)
        }
    }

    return jsonify(resposta),200

@app.route('/',methods=['GET'])
def hello():
    return "<h1>Lab06 - Webservices</h1><h2>Diego Souza Lima Marques - TIA: 32039921</h2><h2>Lucas de Camargo Gonçalves - TIA: 32074964</h2>"
    

if __name__ == '__main__':
 app.run()