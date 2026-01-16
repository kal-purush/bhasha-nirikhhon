from datetime import timedelta

from flask import Flask
#from flask_cors import CORS

from src.servicios.RutasCodigoDescuento import rutas_codigo
from src.servicios.RutasMiembroOfercompas import rutas_miembro
from src.servicios.RutasMultimedia import rutas_multimedia
from src.servicios.RutasOferta import rutas_oferta
from src.servicios.RutasPublicacion import rutas_publicacion

app = Flask(__name__)
#CORS(app, supports_credentials=True)
app.register_blueprint(rutas_miembro)
app.register_blueprint(rutas_oferta)
app.register_blueprint(rutas_publicacion)
app.register_blueprint(rutas_codigo)
app.register_blueprint(rutas_multimedia)
app.config["SECRET_KEY"] = "beethoven"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(minutes=120)


@app.route('/')
def hello_world():
    return 'Hola mundo!'


if __name__ == '__main__':
    #app.run(host="192.168.56.1", port=5000)
    app.run()