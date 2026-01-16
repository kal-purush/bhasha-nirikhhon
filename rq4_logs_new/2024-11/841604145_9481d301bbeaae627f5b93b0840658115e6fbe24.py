from flask import Flask, jsonify, request
from CalcMasaMolar import calcular_masa_molar
from Compuesto import Compuesto

app = Flask(__name__)

@app.route('/mi_api/masa_molar', methods=['POST'])
def masa_molar():
    data = request.json  # Espera datos en formato JSON
    compuesto_input = data.get('formula')  # Obtiene la fórmula desde el JSON
    if not compuesto_input:
        return jsonify({'error': 'Fórmula del compuesto no proporcionada'}), 400

    try:
        # Crea el objeto Compuesto y calcula la masa molar
        #compuesto = Compuesto(compuesto_input)
        mmolar = calcular_masa_molar(compuesto_input)
        
        return {
            'formula': compuesto_input,
            'masa_molar': mmolar
        }
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)