from flask import Flask
from firebase_admin import credentials,initialize_app
from flask_cors import CORS

cred = credentials.Certificate("api/key.json")
default_app=initialize_app(cred)

def create_app():
    app=Flask(__name__)
    app.config['SECRET_KEY'] = '12344gbh743'
    app.config['UPLOAD_FOLDER'] = '../web/public/fimages'

    # Add CORS support
    CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})

    
    from .userapi import userapi
    from .ecgmodel import ecgmodel
    from .heartpatientmodel import heartpatientmodel
    from .pharmaciesapi import pharmaciesapi
    from .dietmodel import dietmodel
    from .medicalrecordsapi import recordapi
    
    app.register_blueprint(userapi,url_prefix='/user')
    app.register_blueprint(ecgmodel,url_prefix='/model')
    app.register_blueprint(heartpatientmodel,url_prefix='/patient')
    app.register_blueprint(pharmaciesapi,url_prefix='/pharmacies')
    app.register_blueprint(dietmodel,url_prefix='/diets')
    app.register_blueprint(recordapi,url_prefix='/medical-records')
    return app
    
    