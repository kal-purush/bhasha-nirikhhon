from flask import Blueprint, jsonify, request
from flask_api import status
from api.constants.folders import models_folder
from api.response.paginate import Pagination
from api.services.storage import decompress_and_save
from config import db
from db.models import Model

model_bp = Blueprint("model", __name__, url_prefix="/api/v1/models")

@model_bp.route("", methods=['POST'])
def register():
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
    args = request.args
    page = args.get("page", default=1, type=int)
    size = args.get("size", default=10, type=int)
    pagination = db.session.query(Model).paginate(page=page, per_page=size, error_out=False)
    response = Pagination(pagination.page, pagination.pages, pagination.items)
    return jsonify(response.serialize()), status.HTTP_200_OK