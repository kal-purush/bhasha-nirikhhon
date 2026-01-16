from flask import Flask, request, jsonify
import pymysql
import boto3 

app = Flask(__name__)

ssm = boto3.client('ssm')


def get_parameter(name, with_decryption=False):
    response = ssm.get_parameter(Name=name, WithDecryption=with_decryption)
    return response['Parameter']['Value']


db_endpoint = get_parameter('DB_URL')        
username = get_parameter('username')         
password = get_parameter('db_password', with_decryption=True) 
db_name = get_parameter('db_name')    

print( password)
# Connect to the MySQL database
def get_db_connection():
    try:
        connection = pymysql.connect(
            host=db_endpoint,
            user=username,
            password=password,
            database=db_name
        )
        print("Connection to MySQL database established successfully.")
        return connection
    except pymysql.MySQLError as e:
        print(f"Error while connecting to MySQL: {e}")
        return None

# Endpoint to add a new string to the database
@app.route('/add', methods=['POST'])
def add_string():
    data = request.json
    new_string = data.get('string')

    if new_string:
        connection = get_db_connection()
        if connection is None:
            return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500
        
        try:
            cursor = connection.cursor()
            # Insert the new string into the database
            cursor.execute("INSERT INTO strings_table (string_value) VALUES (%s)", (new_string,))
            connection.commit()
            return jsonify({'status': 'success', 'message': 'String added successfully'}), 201
        except pymysql.MySQLError as e:
            print(f"Error while inserting string: {e}")
            return jsonify({'status': 'error', 'message': 'Failed to add string'}), 500
        finally:
            cursor.close()
            connection.close()
    return jsonify({'status': 'error', 'message': 'No string provided'}), 400

# Endpoint to retrieve all strings from the database
@app.route('/strings', methods=['GET'])
def get_strings():
    connection = get_db_connection()
    if connection is None:
        return jsonify({'status': 'error', 'message': 'Database connection failed'}), 500

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT string_value FROM strings_table")
        strings = cursor.fetchall()
        return jsonify([s[0] for s in strings])
    except pymysql.MySQLError as e:
        print(f"Error while fetching strings: {e}")
        return jsonify({'status': 'error', 'message': 'Failed to retrieve strings'}), 500
    finally:
        cursor.close()
        connection.close()

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)