from flask import Flask, request, jsonify, send_file
import pymysql
import pyotp
import qrcode
import io
import jwt
import bcrypt
from datetime import datetime, timedelta
from functools import wraps

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your_secret_key'

db = pymysql.connect(
    host='localhost',
    user='root',
    password='',  # Update with your MySQL password if set
    database='auth_db',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

# Helper function to execute queries
def query_db(query, args=(), fetchone=False):
    with db.cursor() as cursor:
        cursor.execute(query, args)
        if fetchone:
            return cursor.fetchone()
        return cursor.fetchall()

def modify_db(query, args=()):
    with db.cursor() as cursor:
        cursor.execute(query, args)
        db.commit()

# JWT Token Required Decorator
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('x-access-token')
        if not token:
            return jsonify({'message': 'Token is missing!'}), 403
        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            user = query_db("SELECT id FROM users WHERE username=%s", (data['username'],), fetchone=True)
            if not user:
                return jsonify({'message': 'Invalid token!'}), 403
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token has expired!'}), 403
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid token!'}), 403
        return f(*args, **kwargs)
    return decorated

# User Registration
@app.route('/register', methods=['POST'])
def register():
    data = request.json
    username = data['username']
    password = bcrypt.hashpw(data['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    secret = pyotp.random_base32()
    modify_db("INSERT INTO users (username, password, twofa_secret) VALUES (%s, %s, %s)", (username, password, secret))
    return jsonify({'message': 'User registered successfully', '2fa_secret': secret})

# Generate 2FA QR Code
@app.route('/generate-2fa/<username>', methods=['GET'])
def generate_2fa(username):
    user = query_db("SELECT twofa_secret FROM users WHERE username=%s", (username,), fetchone=True)
    if not user:
        return jsonify({'message': 'User not found'}), 404
    uri = pyotp.totp.TOTP(user['twofa_secret']).provisioning_uri(name=username, issuer_name='Auth_System')
    qr = qrcode.make(uri)
    img = io.BytesIO()
    qr.save(img)
    img.seek(0)
    return send_file(img, mimetype='image/png')

# Verify 2FA Code
@app.route('/verify-2fa/<username>', methods=['POST'])
def verify_2fa(username):
    data = request.json
    user = query_db("SELECT twofa_secret FROM users WHERE username=%s", (username,), fetchone=True)
    if not user:
        return jsonify({'message': 'User not found'}), 404
    totp = pyotp.TOTP(user['twofa_secret'])
    if totp.verify(data['code']):
        return jsonify({'message': '2FA verified successfully'})
    return jsonify({'message': 'Invalid or expired code'}), 401

# Login and JWT Token Generation
@app.route('/login', methods=['POST'])
def login():
    data = request.json
    user = query_db("SELECT password, twofa_secret FROM users WHERE username=%s", (data['username'],), fetchone=True)
    if not user or not bcrypt.checkpw(data['password'].encode('utf-8'), user['password'].encode('utf-8')):
        return jsonify({'message': 'Invalid credentials'}), 401
    return jsonify({'message': 'Enter 2FA code'}), 200

@app.route('/login-2fa', methods=['POST'])
def login_2fa():
    data = request.json
    user = query_db("SELECT twofa_secret FROM users WHERE username=%s", (data['username'],), fetchone=True)
    if not user:
        return jsonify({'message': 'User not found'}), 404
    totp = pyotp.TOTP(user['twofa_secret'])
    if totp.verify(data['code']):
        token = jwt.encode({'username': data['username'], 'exp': datetime.utcnow() + timedelta(minutes=10)}, app.config['SECRET_KEY'], algorithm='HS256')
        return jsonify({'token': token})
    return jsonify({'message': 'Invalid or expired code'}), 401

# CRUD Operations for Products
@app.route('/products', methods=['POST'])
@token_required
def create_product():
    data = request.json
    modify_db("INSERT INTO products (name, description, price, quantity) VALUES (%s, %s, %s, %s)", (data['name'], data['description'], data['price'], data['quantity']))
    return jsonify({'message': 'Product created successfully'})

@app.route('/products', methods=['GET'])
@token_required
def get_products():
    products = query_db("SELECT * FROM products")
    return jsonify(products)

@app.route('/products/<int:product_id>', methods=['PUT'])
@token_required
def update_product(product_id):
    data = request.json
    modify_db("UPDATE products SET name=%s, description=%s, price=%s, quantity=%s WHERE id=%s", (data['name'], data['description'], data['price'], data['quantity'], product_id))
    return jsonify({'message': 'Product updated successfully'})

@app.route('/products/<int:product_id>', methods=['DELETE'])
@token_required
def delete_product(product_id):
    modify_db("DELETE FROM products WHERE id=%s", (product_id,))
    return jsonify({'message': 'Product deleted successfully'})

if __name__ == '__main__':
    app.run(debug=True)