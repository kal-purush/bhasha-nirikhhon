from flask import Flask, request, jsonify
import pandas as pd
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Carregar o dataset de receitas
df_receitas = pd.read_csv('receitas.csv')


@app.route('/recomendar', methods=['POST'])
def recomendar():
    data = request.get_json()
    dificuldade = data['dificuldade']
    tipo = data['tipo']

    # Filtrar receitas com base na dificuldade e tipo informados
    receitas_filtradas = df_receitas[
        (df_receitas['difficulty'] == dificuldade) & (
            df_receitas['recipe_type'] == tipo)
    ]

    if len(receitas_filtradas) == 0:
        return jsonify({'message': 'Nenhuma receita encontrada com os critérios informados.'})

    # Obter os títulos, forma de preparo e tempo de preparo das receitas recomendadas
    receitas_recomendadas = receitas_filtradas[[
        'title', 'instructions', 'prep_time']].to_dict(orient='records')

    return jsonify({'receitas_recomendadas': receitas_recomendadas})


if __name__ == '__main__':
    app.run()