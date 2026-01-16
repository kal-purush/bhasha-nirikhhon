from flask import Flask, request, Response, render_template, send_from_directory    # New

from validators import validate_and_save
from forms import SignUpForm
from utils import query_flights, encode_jwt
from constants import SECRET_KEY
from mongo_infro import users_coll

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY
app.config['CLIENT_JWT_TOKEN'] = './static/client/jwt_tokens'               # New



@app.route('/signup', methods=['GET', 'POST'])
def signup():
    form = SignUpForm()
    if form.validate_on_submit():
        if users_coll.find_one({'email': form.email.data}):     # проверка есть ли пользователь в базе
            form.email.errors.append('This email already exist')
        else:
            query = {'name': form.name.data, 'password': form.password.data}
            insert_res = users_coll.insert_one(query)
            user_id = str(insert_res.inserted_id)
            jwt_token = encode_jwt(user_id, key=app.config.get('SECRET_KEY'))
            # New
            with open(f'./static/client/jwt_tokens/jwt_token.txt', 'w') as file:
                file.write(jwt_token)
            return send_from_directory(directory=app.config['CLIENT_JWT_TOKEN'],
                                       path='./static/client/jwt_tokens',
                                       filename='jwt_token.txt',
                                       as_attachment=True)

    return render_template('signup.jinja', form=form)

# 127.0.0.1:3000/add_flight
@app.route('/add_flight', methods=['POST'])
def add_flight():
    status_code, errors = validate_and_save(json_object=request.json, object_type='flight')
    return Response(response=errors, status=status_code, mimetype='application/json')


@app.route('/get_flights', methods=['GET'])
def get_flights():
    flights = query_flights()
    return Response(response=flights, mimetype='application/json')


@app.route('/add_ac', methods=['POST'])
def add_ac():
    status_code, errors = validate_and_save(json_object=request.json, object_type='ac')
    return Response(response=errors, status=status_code, mimetype='application/json')


if __name__ == '__main__':
    app.run(port=3000, debug=True)