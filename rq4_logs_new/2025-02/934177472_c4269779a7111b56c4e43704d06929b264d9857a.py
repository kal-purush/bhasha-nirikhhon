from flask import Flask, jsonify, request
from flask_migrate import Migrate
from model import db, TokenBlocklist
from flask_jwt_extended import JWTManager
from datetime import timedelta
from flask_cors import CORS
from authlib.integrations.flask_client import OAuth


app = Flask(__name__)

oauth = OAuth(app)

CORS(app)
# migration initialization
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///help.db'
migrate = Migrate(app, db)
db.init_app(app)


# social
app.config['GITHUB_CLIENT_ID'] = "Ov23liRe9UJhRA4BjO11"
app.config['GITHUB_CLIENT_SECRET'] = "d3de645cc97f0e25080388028ef5385173f0ed93"



# Jwt
app.config["JWT_SECRET_KEY"] = "wtfghgdfghfhb" 
app.config["JWT_ACCESS_TOKEN_EXPIRES"] =  timedelta(hours=1)

jwt = JWTManager(app)
jwt.init_app(app)



from views import *

app.register_blueprint(user_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(faq_bp)
app.register_blueprint(notification_bp)
app.register_blueprint(problem_bp)
app.register_blueprint(solution_bp)
app.register_blueprint(tag_bp)
app.register_blueprint(vote_bp)



@jwt.token_in_blocklist_loader
def check_if_token_revoked(jwt_header, jwt_payload: dict) -> bool:
    jti = jwt_payload["jti"]
    token = db.session.query(TokenBlocklist.id).filter_by(jti=jti).scalar()

    return token is not None









if __name__ == '__main__':
    app.run(debug=True)