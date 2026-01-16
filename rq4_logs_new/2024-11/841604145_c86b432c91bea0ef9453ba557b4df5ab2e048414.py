from flask import Flask, jsonify, request
from Reaccion import Reaccion  
import chempy as ch

app = Flask(__name__)

# endpoint de la API para el balanceo de ecuaciones
@app.route('/mi_api/balancear_reaccion', methods=['POST'])
def balancear_reaccion():
    # datos de la solicitud en formato JSON
    data = request.json
    reaccion_str = data.get('reaccion')  # Obtiene la reacción como string

    if not reaccion_str:
        return jsonify({'error': 'Reacción no proporcionada'}), 400

    #instancia de Reaccion con el string de la reacción
    try:
        reaccion = Reaccion(reaccion_str)
    except Exception as e:
        return jsonify({'error': 'Error al crear la reacción: ' + str(e)}), 400

    # Llama al método Balancear para balancear la reacción
    try:
        resultado_balanceo = reaccion.Balancear()
    except Exception as e:
        # Manejo de errores en caso de que falle el balanceo
        return jsonify({'error': 'Error al balancear la reacción: ' + str(e)}), 500

    return jsonify({
        'reaccion_original': reaccion_str,
        'reaccion_balanceada': resultado_balanceo
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)