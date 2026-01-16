from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return render_template("index.html", title="Alarm Central With ARM", author="Desenvolvido por Gabriel")

@app.route('/cirene', methods=['POST'])
def cirene():
    # Implementar código para ligar e desligar a cirene
    return "Cirene"

@app.route('/sensor-porta', methods=['POST'])
def sensor_porta():
    # Implementar código para ligar e desligar sensores de movimento da porta
    return "Sensor de Movimento da Porta"

@app.route('/sensor-quarto', methods=['POST'])
def sensor_quarto():
    # Implementar código para ligar e desligar sensores de movimento do quarto
    return "Sensor de Movimento do Quarto"

@app.route('/sensor-fundos', methods=['POST'])
def sensor_fundos():
    # Implementar código para ligar e desligar sensores de abertura da porta dos fundos
    return "Sensor de Abertura da Porta dos Fundos"

@app.route('/sensor-entrada', methods=['POST'])
def sensor_entrada():
    # Implementar código para ligar e desligar sensores de abertura da porta de entrada
    return "Sensor de Abertura da Porta de Entrada"

@app.route('/camera', methods=['POST'])
def camera():
    # Implementar código para ligar e desligar os sensores de abertura para visualizar câmera de vigilância
    return "Sensor de Abertura para Visualização da Câmera de Vigilância"

if __name__ == '__main__':
    app.run(debug=True)