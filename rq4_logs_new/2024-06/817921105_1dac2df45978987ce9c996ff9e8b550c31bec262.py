from flask import Flask, render_template, request, redirect, url_for, flash, session, send_from_directory
import os
import sqlite3
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

# Create the Flask application instance
app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'default_key_for_development')

# Configure session to use filesystem
app.config['SESSION_TYPE'] = 'filesystem'

# Set session timeout duration (5 minutes)
SESSION_TIMEOUT_MINUTES = 5

# Function to create the database and tables if they don't exist
def create_db():
    conn = sqlite3.connect('SAMdashboard.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS software_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            software_name TEXT NOT NULL,
            primary_contact TEXT,
            req_number TEXT,
            software_spend REAL,
            contract_file_path TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS savings_tracker (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            software_id INTEGER,
            savings_amount REAL,
            saving_description TEXT,
            FOREIGN KEY (software_id) REFERENCES software_data (id)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS software_catalog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_name TEXT NOT NULL,
            software_name TEXT NOT NULL,
            assigned_date TEXT NOT NULL,
            sr_number TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

# Middleware to check session timeout
@app.before_request
def before_request():
    if 'user_id' in session:
        last_active = session.get('_session_last_active', None)
        if last_active:
            # Convert last_active to naive datetime in UTC timezone
            last_active = last_active.replace(tzinfo=None)
            session_timeout = timedelta(minutes=SESSION_TIMEOUT_MINUTES)
            if datetime.utcnow() - last_active > session_timeout:
                session.clear()
                flash('Your session has timed out due to inactivity.', 'warning')
                return redirect(url_for('login'))
        # Store current UTC time as naive datetime
        session['_session_last_active'] = datetime.utcnow()

# Route to serve static files
@app.route('/static/<path:filename>')
def send_static(filename):
    return send_from_directory('static', filename)

# Route to handle home redirect
@app.route('/')
def home():
    return redirect(url_for('login'))

# Route for user login
@app.route('/login', methods=['GET', 'POST'])
def login():
    session.pop('_session_last_active', None)  # Clear session timeout on login page
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']

        conn = sqlite3.connect('SAMdashboard.db')
        c = conn.cursor()
        c.execute('SELECT * FROM users WHERE email = ?', (email,))
        user = c.fetchone()
        conn.close()

        if user and check_password_hash(user[2], password):
            session['user_id'] = user[0]
            flash('Login successful!', 'success')
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid email or password', 'danger')

    return render_template('login.html')

# Route for user registration
@app.route('/register', methods=['GET', 'POST'])
def register():
    session.pop('_session_last_active', None)  # Clear session timeout on registration page
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        hashed_password = generate_password_hash(password, method='sha256')

        try:
            conn = sqlite3.connect('SAMdashboard.db')
            c = conn.cursor()
            c.execute('INSERT INTO users (email, password) VALUES (?, ?)', (email, hashed_password))
            conn.commit()
            conn.close()
            flash('Registration successful! You can now log in.', 'success')
            return redirect(url_for('login'))
        except sqlite3.IntegrityError:
            flash('Email already registered', 'danger')

    return render_template('register.html')

# Route for user dashboard
@app.route('/dashboard')
def dashboard():
    if 'user_id' not in session:
        flash('Please log in to access this page', 'warning')
        return redirect(url_for('login'))

    conn = sqlite3.connect('SAMdashboard.db')
    c = conn.cursor()
    c.execute('SELECT * FROM software_data')
    software_entries = c.fetchall()
    conn.close()
    return render_template('dashboard.html', software_entries=software_entries)

# Route to submit software entry
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))  # Redirect to your login route

@app.route('/submit', methods=['POST'])
def submit():
    if 'user_id' not in session:
        flash('Please log in to access this page', 'warning')
        return redirect(url_for('login'))

    software_name = request.form['software_name']
    primary_contact = request.form['primary_contact']
    req_number = request.form['req_number']
    software_spend = request.form['software_spend']
    file = request.files['contract_file']

    # Save uploaded file
    if file:
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    else:
        file_path = None

    # Insert data into database
    conn = sqlite3.connect('SAMdashboard.db')
    c = conn.cursor()
    c.execute(
        'INSERT INTO software_data (software_name, primary_contact, req_number, software_spend, contract_file_path) VALUES (?, ?, ?, ?, ?)',
        (software_name, primary_contact, req_number, software_spend, file_path))
    conn.commit()
    conn.close()
    flash('Software entry added successfully!', 'success')
    return redirect(url_for('dashboard'))

# Route to handle uploaded files
@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

# Route to edit software entry
@app.route('/edit/<int:id>', methods=['GET', 'POST'])
def edit(id):
    if 'user_id' not in session:
        flash('Please log in to access this page', 'warning')
        return redirect(url_for('login'))

    conn = sqlite3.connect('SAMdashboard.db')
    c = conn.cursor()
    c.execute('SELECT * FROM software_data WHERE id = ?', (id,))
    software_entry = c.fetchone()
    conn.close()

    if request.method == 'POST':
        software_name = request.form['software_name']
        primary_contact = request.form['primary_contact']
        req_number = request.form['req_number']
        software_spend = request.form['software_spend']
        file = request.files['contract_file']

        # Update entry in database
        conn = sqlite3.connect('SAMdashboard.db')
        c = conn.cursor()
        if file:
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            c.execute('UPDATE software_data SET software_name=?, primary_contact=?, req_number=?, software_spend=?, contract_file_path=? WHERE id=?',
                      (software_name, primary_contact, req_number, software_spend, file_path, id))
        else:
            c.execute('UPDATE software_data SET software_name=?, primary_contact=?, req_number=?, software_spend=? WHERE id=?',
                      (software_name, primary_contact, req_number, software_spend, id))
        conn.commit()
        conn.close()
        flash('Software entry updated successfully!', 'success')
        return redirect(url_for('dashboard'))

    return render_template('edit.html', software_entry=software_entry)

# Route for savings tracker
@app.route('/savings_tracker', methods=['GET', 'POST'])
def savings_tracker():
    if 'user_id' not in session:
        flash('Please log in to access this page', 'warning')
        return redirect(url_for('login'))

    conn = sqlite3.connect('SAMdashboard.db')
    c = conn.cursor()

    if request.method == 'POST':
        software_id = request.form['software_id']
        savings_amount = request.form['savings_amount']
        saving_description = request.form['saving_description']

        c.execute('INSERT INTO savings_tracker (software_id, savings_amount, saving_description) VALUES (?, ?, ?)',
                  (software_id, savings_amount, saving_description))
        conn.commit()
        flash('Savings entry added successfully!', 'success')

    c.execute('''
        SELECT st.id, st.software_id, st.savings_amount, st.saving_description, sd.software_name
        FROM savings_tracker st
        INNER JOIN software_data sd ON st.software_id = sd.id
    ''')
    savings_entries = c.fetchall()

    c.execute('SELECT id, software_name FROM software_data')
    software_options = c.fetchall()

    conn.close()

    return render_template('savings_tracker.html', savings_entries=savings_entries, software_options=software_options)

# Function to establish SQLite connection for software catalog
def get_db_connection():
    conn = sqlite3.connect('SAMdashboard.db')
    conn.row_factory = sqlite3.Row
    return conn

# Function to initialize software catalog database (create table if not exists)
def init_db():
    conn = sqlite3.connect('SAMdashboard.db')
    c = conn.cursor()

    # Create software_catalog table if not exists
    c.execute('''CREATE TABLE IF NOT EXISTS software_catalog (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 user_name TEXT NOT NULL,
                 software_name TEXT NOT NULL,
                 assigned_date TEXT NOT NULL,
                 sr_number TEXT NOT NULL
                 )''')

    conn.commit()
    conn.close()

# Call init_db function to initialize software catalog database
init_db()

# Route to render the catalog page with entries and search form
@app.route('/catalog', methods=['GET', 'POST'])
def catalog():
    conn = get_db_connection()

    if request.method == 'POST':
        search_query = request.form.get('search_query', '')
        cursor = conn.execute(
            "SELECT * FROM software_catalog WHERE user_name LIKE ? OR software_name LIKE ? OR sr_number LIKE ?",
            ('%' + search_query + '%', '%' + search_query + '%', '%' + search_query + '%'))
        catalog_entries = cursor.fetchall()
    else:
        cursor = conn.execute("SELECT * FROM software_catalog")
        catalog_entries = cursor.fetchall()

    conn.close()

    # Render the template with catalog_entries
    return render_template('catalog.html', catalog_entries=catalog_entries)

# Ensure the database and tables are created when the script runs
create_db()

# Run the Flask application
if __name__ == '__main__':
    app.config['UPLOAD_FOLDER'] = 'uploads'
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)