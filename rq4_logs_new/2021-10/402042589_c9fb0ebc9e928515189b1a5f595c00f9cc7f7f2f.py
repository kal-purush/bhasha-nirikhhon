from types import SimpleNamespace
import inject, json
from flask import Blueprint, Response, request
from ...application.add_nutritional_value import AddNutritionalValue
from ...application.get_nutritional_value import GetNutritionalValue
from ...application.delete_nutritional_value import DeleteNutritionalValue
from ...domain.nutritional_value import NutritionalValue
from resources.token.token_required_decorator import token_required


@inject.autoparams()
def create_nutritional_value_blueprint(get_nutritional_value: GetNutritionalValue, add_nutritional_value: AddNutritionalValue, delete_nutritional_value: DeleteNutritionalValue) -> Blueprint:
    nutritional_value_blueprint = Blueprint('nutritional_value', __name__)

    @nutritional_value_blueprint.route('/',methods=['GET'], defaults={'shortname': None})
    @nutritional_value_blueprint.route('/<shortname>',methods=['GET'])
    @token_required
    def get(auth_username,shortname) -> Response:
        return json.dumps(get_nutritional_value.execute(shortname))

    @nutritional_value_blueprint.route('/',methods=['POST'])
    @token_required
    def post(auth_username) -> Response:
        return add_nutritional_value.execute(NutritionalValue(json.loads(json.dumps(request.get_json()),object_hook=lambda d: SimpleNamespace(**d).__dict__)))
    
    @nutritional_value_blueprint.route('/',methods=['DELETE'])
    @token_required
    def delete(auth_username) -> Response:
        return delete_nutritional_value.execute(request.get_json().get("shortname"))

    return nutritional_value_blueprint