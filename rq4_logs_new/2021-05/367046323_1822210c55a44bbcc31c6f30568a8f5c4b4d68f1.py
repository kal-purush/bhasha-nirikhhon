from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/info')
def print_json():
    return jsonify({
        "componente": "resourcemanager",
        "descricao": "serve os clientes com os servicos X, Y e Z",
        "versao": "0.1"
    })


def main():
    app.run(host='0.0.0.0', port= 2000)



