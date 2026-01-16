from flask import Flask, render_template
from backend import create_database, create_tables

# Import the blueprints from the backend.routes module
from backend.routes.branch_routes import branch_bp
from backend.routes.customer_routes import customer_bp
from backend.routes.delivery_routes import delivery_bp
from backend.routes.employee_routes import employee_bp
from backend.routes.kpopgroup_routes import kpopgroup_bp
from backend.routes.order_routes import order_bp
from backend.routes.product_routes import product_bp
from backend.routes.shipping_routes import shipping_bp
from backend.routes.warehouse_routes import warehouse_bp

app = Flask(__name__, static_folder='frontend/static', template_folder='frontend/templates')

# Register the blueprints with their respective URL prefixes
app.register_blueprint(branch_bp, url_prefix='/api/branches')
app.register_blueprint(customer_bp, url_prefix='/api/customers')
app.register_blueprint(order_bp, url_prefix='/api/orders')
app.register_blueprint(employee_bp, url_prefix='/api/employees')
app.register_blueprint(shipping_bp, url_prefix='/api/shippings')
app.register_blueprint(warehouse_bp, url_prefix='/api/warehouses')
app.register_blueprint(product_bp, url_prefix='/api/products')
app.register_blueprint(delivery_bp, url_prefix='/api/deliveries')
app.register_blueprint(kpopgroup_bp, url_prefix='/api/kpopgroups')

# Define routes to serve the HTML templates
@app.route('/')
def home():
    return render_template('index.html')

@app.route('/branches')
def branches():
    return render_template('branches.html')

@app.route('/products')
def products():
    return render_template('products.html')

@app.route('/cart')
def cart():
    return render_template('cart.html')

@app.route('/login')
def login():
    return render_template('login.html')

@app.route('/signup')
def signup():
    return render_template('signup.html')

@app.route('/static/<path:path>')
def send_static(path):
    return app.send_static_file(path)

if __name__ == '__main__':
    create_database()  # Create the database first
    create_tables()    # Then create tables
    print("Database, tables, and sample data created successfully")
    app.run(debug=True)