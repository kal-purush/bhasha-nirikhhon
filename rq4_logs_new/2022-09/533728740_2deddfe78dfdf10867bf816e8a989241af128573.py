import os
import urllib.parse as up
from flask import url_for
from flask_jwt_extended import JWTManager
from flask_script import Manager

from src import db, ma, create_app, configs, api

config = os.environ.get('PYTH_SRVR', 'default')

config = configs.get(config)

extensions = [db, ma, api]

app = create_app(__name__, config, extensions=extensions)

jwt = JWTManager(app)

manager = Manager(app)


@manager.shell
def _shell_context():
    return dict(
        app=app,
        db=db,
        ma=ma,
        config=config
    )


@manager.command
def list_routes():
    output = []
    for rule in app.url_map.iter_rules():
        options = {}
        for arg in rule.arguments:
            options[arg] = "[{0}]".format(arg)
        methods = ','.join(rule.methods)
        url = url_for(rule.endpoint, **options)
        line = up.unquote("{:50s} {:20s} {}".format(rule.endpoint, methods, url))
        output.append(line)

    for line in sorted(output):
        print(line)


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    manager.run()