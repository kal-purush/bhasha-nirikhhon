from flask import Flask, render_template, request, redirect, session, flash, url_for, send_from_directory
import pyodbc
import os
from werkzeug.utils import secure_filename

app = Flask(__name__, template_folder='template')
app.secret_key = '7f84c74d14a8602475c8e2f9cbcf3a6a'

# Configuración de carga de archivos
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'mp4'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Asegúrate de que la carpeta de cargas existe
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Conexión a la base de datos SQL Server
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ALVARO\\SQLEXPRESS;'
                      'Database=login;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    return redirect('/login.html')

@app.route('/login.html', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        cursor.execute("SELECT * FROM TBL_USER WHERE username=? AND passwor=?", (username, password))
        user = cursor.fetchone()
        
        if user:
            session['username'] = username
            return redirect('/welcome.html')
        else:
            return render_template('login.html', error='Usuario o contraseña incorrectos')
    else:
        return render_template('login.html', error='')

@app.route('/welcome.html')
def bienvenido():
    if 'username' in session:
        # Listar los archivos en el directorio de subida específico del usuario
        user_folder = os.path.join(app.config['UPLOAD_FOLDER'], session['username'])
        if not os.path.exists(user_folder):
            os.makedirs(user_folder)
        files = os.listdir(user_folder)
        return render_template('welcome.html', username=session['username'], files=files)
    else:
        return redirect('/login.html')

@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect('/login.html')

@app.route('/register.html', methods=['GET', 'POST'])
def registro():
    if request.method == 'POST':
        fullname = request.form['fullname']
        firstname = request.form['firstname']
        username = request.form['username']
        password = request.form['password']

        if len(fullname) <= 1 or len(firstname) <= 1:
            return render_template('register.html', error='Por favor, introduce un nombre y apellido válidos.')
        
        cursor.execute("SELECT * FROM TBL_USER WHERE username=?", (username,))
        existing_user = cursor.fetchone()
        if existing_user:
            return render_template('register.html', error='El nombre de usuario ya está en uso')
        
        cursor.execute("INSERT INTO TBL_USER (NOMBRES, APELLIDOS, USERNAME, PASSWOR) VALUES (?, ?, ?, ?)", 
                       (fullname, firstname, username, password))
        conn.commit()
        
        # Crear directorio para el nuevo usuario
        user_folder = os.path.join(app.config['UPLOAD_FOLDER'], username)
        if not os.path.exists(user_folder):
            os.makedirs(user_folder)
        
        return redirect('/login.html')
    else:
        return render_template('register.html', error='No se agregó')

@app.route('/recover.html', methods=['GET', 'POST'])
def recuperar():
    if request.method == 'POST':
        username = request.form['username']
        
        cursor.execute("SELECT passwor FROM TBL_USER WHERE username=?", (username,))
        password = cursor.fetchone()
        
        if password:
            return render_template('recover.html', password=password[0])
        else:
            return render_template('recover.html', error='El nombre de usuario no está registrado.')
    else:
        return render_template('recover.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'username' not in session:
        return redirect('/login.html')

    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    
    file = request.files['file']
    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        user_folder = os.path.join(app.config['UPLOAD_FOLDER'], session['username'])
        file.save(os.path.join(user_folder, filename))
        flash('File successfully uploaded')
        return redirect('/welcome.html')
    
    flash('File type not allowed')
    return redirect(request.url)

@app.route('/uploads/<username>/<filename>')
def uploaded_file(username, filename):
    user_folder = os.path.join(app.config['UPLOAD_FOLDER'], username)
    return send_from_directory(user_folder, filename)

if __name__ == '__main__':
    app.run(debug=True)