from flask import Flask, jsonify, request  # Importamos Flask y funciones auxiliares para gestionar el API
from Estequiometria.Conversiones import *
from Estequiometria.crearDato import *

app = Flask(__name__)

# endpoint de la API para el balanceo de ecuaciones
@app.route('/mi_api/crear_dato', methods=['POST'])
def crear_dato():
    # datos de la solicitud en formato JSON
    dict = request.json
    
    if not dict:
        # Si no se proporciona la fórmula, retornamos un error 400 (solicitud incorrecta)
        return jsonify({'error': 'Fórmula del compuesto no proporcionada'}), 400

    try:
        #Creamos el dato
        dato = crearDato(dict).__str__()
        
        # Retornamos el resultado en formato JSON
        return jsonify(dato)
    
    except ValueError as e:
        # Capturamos excepciones y devolvemos el error en formato JSON
        return jsonify({'error': str(e)}), 400
    
# Ejecutamos la aplicación en modo de depuración para pruebas locales
if __name__ == '__main__':
    app.run(debug=True)