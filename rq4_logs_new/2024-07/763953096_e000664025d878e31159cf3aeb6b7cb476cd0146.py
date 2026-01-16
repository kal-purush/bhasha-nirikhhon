from flask import Blueprint
from flask_swagger_ui import get_swaggerui_blueprint

SWAGGER_URL="/swagger"
API_URL="/static/swagger.json"

swagger_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'peach':"Access API"
    }
)

# swagger_blueprint = Blueprint('swagger_docs', __name__)