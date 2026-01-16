from flask import Flask, jsonify, request
from Nomenclatura import *


NomenclaturaApp = Flask(__name__) 

@NomenclaturaApp.route("/api/Problema", methods=[POST])
def IonesProblema():
    data = request.json
    nivelIones = data.get("nivel")

    if not nivelIones:
        return jsonify({"error": "Nivel no proporcionado"})

    try:
        problema, id = Nomenclatura.Problema(Nomenclatura.Nivel(nivelIones))
        
        return jsonify({"problema": problema, "id": id})

    except ValueError as e:
        return jsonify({"error": str(e)})


@NomenclaturaApp.route("/api/SolucionProblema", methods=[POST])
def IonesSolucionProblema():
    data = request.json
    problema = data.get("problema")
    solución = data.get("solución")
    id = data.get("id")

    if not solución:
        return jsonify({"error": "Solución no proporcionada"})

    if not  problema:
        return jsonify({"error": "Problema no proporcionado"})

    if not  id:
        return jsonify({"error": "Id no proporcionado"})

    try:
        solucion, comprobante = Nomenclatura.ComprobarRespuesta(solución, problema, id)
        return {"solución": solución,  "comprobante": comprobante}

    except  ValueError as e:
        return jsonify({"error": str(e)})


if __name__ == '__main__':
    app.run(debug=True)