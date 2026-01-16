# Importing necessary libraries
import flask
import json
import jwt
import sqlite3

# Connecting to the database
conn = sqlite3.connect('database.db')

# Initializing the Flask application
app = flask.Flask(_name_)

# Defining the route for the API
@app.route('/api/native_english', methods=['GET', 'POST', 'PUT', 'DELETE'])
def native_english():
    # Checking for authentication
    if not 'Authorization' in flask.request.headers:
        return flask.jsonify({'error': 'Authentication required'}), 401
    else:
        # Decoding the JWT token
        token = jwt.decode(flask.request.headers['Authorization'], 'secret', algorithms=['HS256'])
        # Checking for the request method
        if flask.request.method == 'GET':
            # Fetching the data from the database
            cur = conn.cursor()
            cur.execute('SELECT native_english FROM users WHERE id=?', (token['id'],))
            row = cur.fetchone()
            # Returning the response
            return flask.jsonify({'native_english': row[0]})
        elif flask.request.method == 'POST':
            # Parsing the request body
            data = json.loads(flask.request.data)
            # Updating the data in the database
            cur = conn.cursor()
            cur.execute('UPDATE users SET native_english=? WHERE id=?', (data['native_english'], token['id']))
            conn.commit()
            # Returning the response
            return flask.jsonify({'success': True})
        elif flask.request.method == 'PUT':
            # Parsing the request body
            data = json.loads(flask.request.data)
            # Inserting the data into the database
            cur = conn.cursor()
            cur.execute('INSERT INTO users (id, native_english) VALUES (?, ?)', (token['id'], data['native_english']))
            conn.commit()
            # Returning the response
            return flask.jsonify({'success': True})
        elif flask.request.method == 'DELETE':
            # Deleting the data from the database
            cur = conn.cursor()
            cur.execute('DELETE FROM users WHERE id=?', (token['id'],))
            conn.commit()
            # Returning the response
            return flask.jsonify({'success': True})
        else:
            # Returning the response
            return flask.jsonify({'error': 'Invalid request method'}), 400