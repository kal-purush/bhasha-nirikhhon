from flask import Blueprint, jsonify, request
from flask_api import status
from api.constants.folders import models_folder
from api.services.storage import decompress_and_save
from config import db
from db.models import Model

model_bp = Blueprint("model", __name__, url_prefix="/api/v1/models")

@model_bp.route("", methods=['POST'])
def register():
    content_type = request.headers.get('Content-Type')
    if (content_type != 'multipart/form-data'):
        return 'Content-Type not supported', status.HTTP_415_UNSUPPORTED_MEDIA_TYPE

    model_name = request.form.get('name')
    if model_name == '' :
        return "Nome do modelo é obrigatório", status.HTTP_400_BAD_REQUEST
    
    model_compressed_file = request.files.get('model')
    filename = model_compressed_file.filename
    if filename == "":
        return "Arquivo contendo o modelo é obrigatório", status.HTTP_400_BAD_REQUEST
    
    path_to_model = decompress_and_save(models_folder, model_compressed_file.stream._file, model_compressed_file.filename)
    new_model = Model(
        name = model_name,
        path = path_to_model
    )
    db.session.add(new_model)
    db.session.commit()

    return '', status.HTTP_201_CREATED

@model_bp.route("", methods=['GET'])
def list_models():
    models = db.session.query(Model).all()
    response = list()
    for model in models:
        response.append(model.serialize())
    return jsonify(response), status.HTTP_200_OK