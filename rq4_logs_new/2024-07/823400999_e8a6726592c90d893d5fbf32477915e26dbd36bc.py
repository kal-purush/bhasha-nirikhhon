import os
import json
import logging
import pytz

from cs50 import SQL
from datetime import datetime, timedelta
from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template, request, session, send_from_directory, url_for, g
from flask_babel import Babel, _,lazy_gettext as _l, gettext
from flask_mail import Mail, Message
from flask_session import Session
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
from logging.config import dictConfig
from logging.handlers import RotatingFileHandler
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge

from helpers import apology, login_required, allowed_file, upload_img, upload_doc, update_lg_sg, mark_sec_complete,  \
    mark_course_complete, update_course_final_grade, update_final_lesson, question_count

load_dotenv()

# Determine log file path based on environment
if os.getenv('FLASK_DEBUG') == '0':
    log_file_path = '/server_path'
else:
    log_file_path = 'logs/dev_error.log'


# Configure logging before creating the Flask app (DEBUG logs all levels)
dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {
        'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'default'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': log_file_path,
            'maxBytes': 10240,
            'backupCount': 10,
            'formatter': 'default'
        }
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['wsgi', 'file']
    }
})


# Configure application
app = Flask(__name__)
app.config['UPLOAD_IMG_DIRECTORY'] = 'static/images/uploads/'
app.config['UPLOAD_DOC_DIRECTORY'] = 'static/docs/uploads/'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 # 16MB
app.config['ALLOWED_IMAGE_EXTENSIONS'] = ['.jpg', '.jpeg', '.png', '.gif']
app.config['ALLOWED_DOC_EXTENSIONS'] = ['.pdf']


# Email configuration
app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER')
app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT'))
app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS') == 'True'
app.config['MAIL_USE_SSL'] = os.getenv('MAIL_USE_SSL') == 'True'
app.config['MAIL_DEFAULT_SENDER'] = (os.getenv('MAIL_DEFAULT_SENDER_NAME'), os.getenv('MAIL_DEFAULT_SENDER_EMAIL'))

app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')

mail = Mail(app)

# Configure Babel
app.config['BABEL_DEFAULT_LOCALE'] = 'en'
app.config['BABEL_TRANSLATION_DIRECTORIES'] = './translations'
babel = Babel(app)


# Configure session to use filesystem (instead of signed cookies)
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
Session(app)


# Configure CS50 Library to use SQLite database
db = SQL(os.getenv('DATABASE_URL'))

@app.after_request
def after_request(response):
    """Ensure responses aren't cached"""
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Expires"] = 0
    response.headers["Pragma"] = "no-cache"
    return response

# ------------- LOGGING DEBUG START -------------

@app.errorhandler(500)
def internal_error(exception):
    app.logger.error(exception)

@app.route('/test', methods=['GET'])
def test():
    app.logger.info('Test route called')
    raise Exception('This is a test exception')

# Error checking for logs:
@app.route('/error', methods=['GET'])
def error():
    app.logger.info('Error route called')
    raise Exception("This is a test error!")

@app.route('/debug-status', methods=['GET'])
def debug_status():
    app.logger.info('Debug status route called')
    return f"Debug mode is {'on' if app.debug else 'off'}"

@app.route('/example', methods=['GET'])
def example_route():
    app.logger.debug('This is a debug message')
    app.logger.info('This is an info message')
    app.logger.warning('This is a warning message')
    app.logger.error('This is an error message')
    app.logger.critical('This is a critical message')
    return "Check the logs for different levels of logging."

# ------------- LOGGING DEBUG END -------------

# ------------- BABEL START -------------

def get_locale():
    # Check if we have language stored in the session
    if 'lang' in session:
        # print(f"Language retrieved from session: {session['lang']}")  # Debug statement
        return session.get('lang')
    # Otherwise, use the browser's preferred language
    else:
        browser_lang = request.accept_languages.best_match(['en', 'fr'])
        # print(f"Language set via browser preference: {browser_lang}")  # Debug statement
        return browser_lang


def get_timezone():
    user = getattr(g, 'user', None)
    if user is not None:
        return user.timezone


babel = Babel(app, locale_selector=get_locale, timezone_selector=get_timezone)


@app.context_processor
def inject_babel():
    return dict(_=gettext)


@app.context_processor
def inject_locale():
    # This makes the function available directly, allowing you to call it in the template
    return {'get_locale': get_locale}


# If already logged in
@app.route('/setlang', methods=['GET', 'POST'])
def setlang():
    lang = request.form.get('lang', 'en')
    session['lang'] = lang
    # print(f"...SETLANG - Session: {session}") # Debug print

    return redirect(url_for('index'))


# If registering
@app.route('/setlang_register', methods=['GET', 'POST'])
def setlang_register():
    lang = request.form.get('lang', 'en')
    session['lang'] = lang
    # print(f"...SETLANG REGISTER - Session: {session}") # Debug print
    return redirect(url_for('register'))


# If logged in already
@app.route('/language')
def language():
    return render_template('language.html')

# -------------BABEL END -------------

# -------- MAIL START -----------

# ChatGPT's Password reset code 
@app.route('/reset_request', methods=['GET', 'POST'])
def reset_request():
    if request.method == 'POST':
        email = request.form.get('email')
        user = db.execute("SELECT email FROM users WHERE email = ?", (email,))
        if user:
            token = generate_reset_token(email)
            send_reset_email(email, token)
            flash('An email has been sent with instructions to reset your password. Please check your spam!', 'info')
            return redirect(url_for('login'))
        else:
            flash('Email address not found.', 'warning')
    return render_template('reset_request.html')


# ChatGPT's Token for resetting password
def generate_reset_token(email):
    # from itsdangerous import URLSafeTimedSerializer
    serializer = URLSafeTimedSerializer(app.config['SECRET_KEY'])
    return serializer.dumps(email, salt='password-reset-salt')


# ChatGPT's Reset password email
def send_reset_email(to_email, token):
    message = Message('Password Reset Request',
                 recipients=[to_email])
    message.body = f'''To reset your password, visit the following link:
    {url_for('reset_token', token=token, _external=True)}

    If you did not make this request then simply ignore this email and no changes will be made.
    '''
    mail.send(message)


# ChatGPT's Route for password reset form
@app.route('/reset_password/<token>', methods=['GET', 'POST'])
def reset_token(token):
    # from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
    try:
        serializer = URLSafeTimedSerializer(app.config['SECRET_KEY'])
        email = serializer.loads(token, salt='password-reset-salt', max_age=3600)  # Token valid for 1 hour
    except (SignatureExpired, BadSignature):
        flash('The password reset link is invalid or has expired.', 'warning')
        return redirect(url_for('reset_request'))

    if request.method == 'POST':
        password = request.form.get('password')
        confirmation = request.form.get('confirmation')
        if password != confirmation:
            return apology("Passwords do NOT match", 400)
            # flash('Passwords do not match, please re-enter your password and confirmation', 'warning')
        if password:
            hashed_password = generate_password_hash(password, method='pbkdf2:sha256:600000', salt_length=16)

            # Update new password into users database
            db.execute("UPDATE users SET hash = :hash WHERE email = :email", hash=hashed_password, email=email)
            flash('Your password has been updated!', 'success')

            return redirect(url_for('login'))
            
    return render_template('reset_password.html')

# -------- MAIL END -----------

# -------- UPLOADS START -----------

@app.route('/serve-image/<filename>', methods=['GET'])
def serve_image(filename):
    return send_from_directory(app.config['UPLOAD_IMG_DIRECTORY'], filename)


@app.route('/serve-pdf/<filename>', methods=['GET'])
def serve_pdf(filename):
    return send_from_directory(app.config['UPLOAD_DOC_DIRECTORY'], filename)


@app.route("/pdf_upload", methods=["POST"])
@login_required
def pdf_upload():
    file = request.files.get('file')
    if file and file.filename != '':
        pdf_file = upload_doc(file, app.config['ALLOWED_PDF_EXTENSIONS'])
        if not pdf_file:
            return apology("File upload failed or file is not allowed", 400)
        # Handle the pdf file (e.g., save the filename to the database)
    else:
        return apology("No file selected", 400)

    # Redirect or render a template as needed
    return redirect("/courses")

# -------- UPLOADS END -----------

@app.route("/")
@login_required
def index():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    rows = db.execute("SELECT * FROM users WHERE id = ?", users_id)
    # valid_user = len(rows)

    # if valid_user != 0:
    if rows:
        
        user = rows[0]
        # print(f"...USER: {user}")

        return render_template("index.html", users=user)

    # If not valid user, send to register
    else:
        return render_template("register.html")
    
@app.route("/register_lang", methods=["GET", "POST"])
def register_lang():
    """Register user language"""
    # Forget any user_id
    session.clear()
    return render_template("register_lang.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    """Register user"""

    if request.method == "POST":

        # Check Access Code is valid
        code = request.form.get("code")
        if not code:
            return apology("Please enter your access code", 400)
        
        user_code = code.lower()
        
        code_search = db.execute("SELECT * FROM access_codes WHERE code = ?", user_code)
        valid_code = len(code_search)

        # if valid code
        if valid_code != 0:

            # Get role id associated with code
            role_id = code_search[0]['access_role_id']

            #  Get role associated with role id
            new_role = db.execute("SELECT * FROM roles WHERE role_id = ?", role_id)
            role = new_role[0]['role']

            # Add the user's entry into the database
            first_name = request.form.get("first_name")
            if not first_name:
                return apology("Please enter First Name", 400)

            last_name = request.form.get("last_name")
            if not last_name:
                return apology("Please enter Last Name", 400)  

            email = request.form.get("email")
            if not email:
                return apology("Please enter Email", 400)
            if '.' not in email:
                    return apology("Please enter complete Email address", 400)
            
            username = request.form.get("username")
            if not username:
                return apology("Please enter Username", 400)

            password = request.form.get("password")
            if not password:
                return apology("Please enter Password", 400)

            confirmation = request.form.get("confirmation")
            if not confirmation:
                return apology("Please confirm Password", 400)
            elif confirmation != password:
                return apology("Passwords do NOT match", 400)

            # Query database for username
            rows = db.execute(
                "SELECT * FROM users WHERE username = ?", username
            )
            
            # Ensure username doesn't already exist
            if len(rows) != 0:
                return apology("Username already exists", 400)
            
            # Query database for email
            row = db.execute(
                "SELECT * FROM users WHERE email = ?", email
            )
            
            # Ensure email doesn't already exist
            if len(row) != 0:
                return apology("Email already exists", 400)
            
            # Hash password
            hash = generate_password_hash(password, method='pbkdf2:sha256:600000', salt_length=16)

            # Store new user info into users database
            db.execute("""INSERT INTO users (first_name, last_name, email, username, hash, role_id, role) VALUES (?, ?, ?, ?, ?, ?, ?)""", 
                       first_name, last_name, email, username, hash, role_id, role)

            # Register the new user
            user_rows = db.execute("SELECT * FROM users WHERE username = ?", username)
            username_id = user_rows[0]['id']
            
            db.execute("INSERT INTO registration (registration_user_id) VALUES (?)", username_id)

            # Check if user has profile
            row = db.execute("SELECT * FROM user_profiles WHERE profile_id = ?", username_id)
            has_profile = len(row)
            
            #if new profile
            if has_profile == 0:
                # Create new user profile
                db.execute("INSERT INTO user_profiles (profile_id) VALUES (?)", username_id)

            else:    
                return apology("Profile already exists", 400)
            
            # Get user's language preference from session
            if session['lang'] == 'en':
                language = 1
            else: 
                language = 2

            # Store user's language pref in user's profile
            db.execute("UPDATE user_profiles SET user_language_id = ? WHERE profile_id = ?", language, username_id)
            
            # If a student, automatically enroll them into their new program
            role_id_int = int(role_id)
            if role_id_int > 3:

                # Retrieve Program ID based on User Role
                id = db.execute("SELECT program_id FROM programs WHERE program = ?", role)
                program_id = id[0]['program_id']

                # Update user profile with program_id
                db.execute("UPDATE user_profiles SET user_program_id = ? WHERE profile_id = ?", program_id, username_id)
                
                # Enroll user into program
                db.execute("INSERT INTO program_enrollment (enrollment_user_id, enrollment_program_id) VALUES (?, ?)", username_id, program_id)

            # Determine which login page to show:
            if session['lang'] == 'en':
                return redirect("/login")
            
            else:
                return redirect("/fr_login")
        
        else:
            return apology("Please enter valid access code", 400)

    # User reached route via GET (as by clicking a link or via redirect)
    else:
        return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    """Log user in"""

    # Forget any user_id
    session.clear()

    # User reached route via POST (as by submitting a form via POST)
    if request.method == "POST":

        # Ensure username was submitted
        if not request.form.get("username"):
            return apology("must provide username", 403)

        # Ensure password was submitted
        elif not request.form.get("password"):
            return apology("must provide password", 403)

        # Query database for username
        rows = db.execute(
            "SELECT * FROM users WHERE username = ?", request.form.get("username"))

        # Ensure username exists and password is correct
        if len(rows) != 1 or not check_password_hash(
            rows[0]["hash"], request.form.get("password")):
            return apology("invalid username and/or password", 403)

        # Remember which user has logged in
        session["user_id"] = rows[0]["id"]

         # Lookup user's role, grant dashboard access
        user_role = rows[0]['role_id']

        # Get username
        user_id = rows[0]['id']

        # Set User's language preference in session
        pref = db.execute("SELECT user_language_id FROM user_profiles WHERE profile_id = ?", user_id)
        # print(f"...LOGIN - Lang Pref {lang_pref}") # Debug
        lang_pref = pref[0]['user_language_id']
        if lang_pref == 2:
            session['lang'] = 'fr'
        else:
            session['lang'] = 'en'
        
        # ADMIN DASHBOARD
        if user_role == 1:
            return render_template("admin_dash.html")

        # INSTRUCTOR DASHBOARD
        if user_role == 2:
            return render_template("instructor_dash.html")

        # COORDINATOR DASHBOARD
        if user_role == 3:
            return render_template("coordinator_dash.html")
        
        # CIGCA NORTH DASHBOARD
        if user_role == 4:
            return render_template("cigca_north_dash.html")
        
        # CIGCA SOUTH DASHBOARD
        if user_role == 5:
            return render_template("cigca_south_dash.html")

        # CIGCA EAST DASHBOARD
        if user_role == 6:
            return render_template("cigca_east_dash.html")
        
        # CIGCA WEST DASHBOARD
        if user_role == 7:
            return render_template("cigca_west_dash.html")

        # CIGCA CENTRAL DASHBOARD
        if user_role == 8:
            return render_template("cigca_central_dash.html")

        # UPF DASHBOARD
        if user_role == 9:
            return render_template("upf_dash.html")

        # RISE DASHBOARD
        if user_role == 10:
            return render_template("rise_dash.html")
        
        # Redirect user to home page
        else:
            return apology("You do not have an assigned role", 400)
            

    # User reached route via GET (as by clicking a link or via redirect)
    else:
        return render_template("login.html")


@app.route("/logout")
def logout():
    """Log user out"""

    # Forget any user_id
    session.clear()

    # Redirect user to login form
    return redirect("/")

@app.route("/admin_dash")
@login_required
def admin_dash():
    """Show Dashboard"""

   # Ensure valid user
    users_id = session["user_id"]
    rows = db.execute("SELECT * FROM users WHERE id = ?", users_id)
    user_role = rows[0]['role_id']

    if user_role == 1:

        return render_template("admin_dash.html", users=rows)
    
    else:
        return redirect("register.html")
    

@app.route("/instructor_dash", methods=["GET", "POST"])
@login_required
def instructor_dash():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    rows = db.execute("SELECT * FROM users WHERE id = ?", users_id)
    user_role = rows[0]['role_id']

    if user_role == 2:

        return render_template("instructor_dash.html")
    
    else:
        return render_template("register.html")


@app.route("/coordinator_dash", methods=["GET", "POST"])
@login_required
def coordinator_dash():
    """Show Dashboard"""

   # Ensure valid user
    users_id = session["user_id"]
    rows = db.execute("SELECT * FROM users WHERE id = ?", users_id)
    user_role = rows[0]['role_id']

    if user_role == 3:

        return render_template("coordinator_dash.html")
    
    else:
        return render_template("register.html")
    

@app.route("/cigca_north_dash", methods=["GET", "POST"])
@login_required
def cigca_north_dash():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    rows = db.execute("SELECT * FROM users WHERE id = ?", users_id)
    user_role = rows[0]['role_id']

    if user_role == 4:

        return render_template("cigca_north_dash.html")
    
    else:
        return render_template("register.html")
    

@app.route("/cigca_south_dash", methods=["GET", "POST"])
@login_required
def cigca_south_dash():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    rows = db.execute("SELECT * FROM users WHERE id = ?", users_id)
    user_role = rows[0]['role_id']

    if user_role == 5:

        return render_template("cigca_south_dash.html")
    
    else:
        return render_template("register.html")
    

@app.route("/cigca_east_dash", methods=["GET", "POST"])
@login_required
def cigca_east_dash():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    rows = db.execute("SELECT * FROM users WHERE id = ?", users_id)
    user_role = rows[0]['role_id']

    if user_role == 6:

        return render_template("cigca_east_dash.html")
    
    else:
        return render_template("register.html")
    

@app.route("/cigca_west_dash", methods=["GET", "POST"])
@login_required
def cigca_west_dash():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    rows = db.execute("SELECT * FROM users WHERE id = ?", users_id)
    user_role = rows[0]['role_id']

    if user_role == 7:

        return render_template("cigca_west_dash.html")
    
    else:
        return render_template("register.html")
    

@app.route("/cigca_central_dash", methods=["GET", "POST"])
@login_required
def cigca_central_dash():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 8:

        return render_template("cigca_central_dash.html")
    
    else:
        return render_template("register.html")
    

@app.route("/upf_dash", methods=["GET", "POST"])
@login_required
def upf_dash():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 9:

        return render_template("upf_dash.html")
    
    else:
        return render_template("register.html")
    

@app.route("/rise_dash", methods=["GET", "POST"])
@login_required
def rise_dash():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 10:

        return render_template("rise_dash.html")
    
    else:
        return render_template("register.html")
    

# """ ----------------------- USERS START ----------------------- ""

@app.route("/users")
@login_required
def users():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:
        
        # Flag for edit button template
        flag = True

        # Display the entries in the database on users.html
        rows = db.execute("SELECT * FROM users JOIN user_profiles ON user_profiles.profile_id = users.id")

        return render_template("users.html", users=rows, no_edit=flag)
    
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/edit", methods=["POST"])
@login_required
def edit():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for edit button template
        flag = True

        # Options of Roles to Select
        roles = db.execute("SELECT * FROM roles")

        #  Person to edit
        id = request.form.get("edit_id")
        if id:

            users = db.execute(
                """SELECT * FROM users JOIN user_profiles ON users.id = user_profiles.profile_id WHERE id = ?""", id)
            # print(f"....users:{users}")
                
            return render_template("users.html", yes_edit=flag, roles=roles, users=users, edit_user_id=id)
        
        else:
            return apology("You do not have permission to access this page", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/user_edit_confirm", methods=["POST"])
@login_required
def user_edit_confirm():


     # Ensure valid admin user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for edit button template
        flag = True

        #  Person to edit
        id = request.form.get("user_edit_id")
        if id:
            
            # Get new values for User (Ensure foreign keys have value=id)
            username = request.form.get("inputUsername")
            first_name = request.form.get("inputFirstName")
            last_name = request.form.get("inputLastName")
            
            email = request.form.get("inputEmail")
            if not email:
                return apology("Please enter Email", 400)
            if '.' not in email:
                    return apology("Please enter complete Email address", 400)
            
            role_id = request.form.get("inputRole")
            phone_number = request.form.get("inputPhone")

            # Translate role
            role_search = db.execute("SELECT role FROM roles WHERE role_id = ?", role_id)
            role = role_search[0]['role']

            #  Update User
            db.execute("""
                        UPDATE users SET (username, first_name, last_name, email, role_id, role) 
                        = (?, ?, ?, ?, ?, ?) 
                        WHERE id = ?""", username, first_name, last_name, email, role_id, role, id)
            
            # UPdate Profile
            db.execute("UPDATE user_profiles SET phone_number = ? WHERE profile_id = ?", phone_number, id)

            # If a student, automatically enroll them into their new program
            role_id_int = int(role_id)
            if role_id_int > 3:

                # Retrieve Program ID based on User New Role
                program_title_id = db.execute("SELECT program_id FROM programs WHERE program = ?", role)
                program_id = program_title_id[0]['program_id']

                # Update user profile with program_id
                db.execute("UPDATE user_profiles SET user_program_id = ? WHERE profile_id = ?", program_id, id)
                
                # Enroll user into program
                db.execute("INSERT INTO program_enrollment (enrollment_user_id, enrollment_program_id) VALUES (?, ?)", id, program_id)

            # Display updated entries in the database on users.html
            # new_rows = db.execute("SELECT * FROM users JOIN user_profiles ON user_profiles.profile_id = users.id")
                    
            return redirect("/users")
        
        else:
            return apology("User not found, Please define User id", 400)
        
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/add_user", methods=["POST"])
@login_required
def add_user():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:
        
        # Flag for add button template
        flag = True

        roles = db.execute("SELECT * FROM roles")

        return render_template("users.html", yes_add=flag, roles=roles)
    
    else:
        return apology("You do not have permission to access this page", 400)


@app.route("/user_add_confirm", methods=["POST"])
@login_required
def user_add_confirm():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Get new user information (Ensure foreign keys have value=id)
        first_name = request.form.get("inputFirstName")
        if not first_name:
            return apology("Please enter First Name", 400)
        
        last_name = request.form.get("inputLastName")
        if not last_name:
            return apology("Please enter Last Name", 400)
        
        email = request.form.get("inputEmail")
        if not email:
            return apology("Please enter Email", 400)
        if '.' not in email:
                    return apology("Please enter complete Email address", 400)
        
        phone_number = request.form.get("inputPhone")
        
        username = request.form.get("inputUsername")
        if not username:
            return apology("Please enter Username", 400)
        
        password = request.form.get("inputPassword")
        if not password:
            return apology("Please enter Password", 400)
        
        confirmation = request.form.get("inputPasswordConfirm")
        if not confirmation:
            return apology("Please confirm Password", 400)
        elif confirmation != password:
            return apology("Passwords do NOT match", 400)
        
        role_id = request.form.get("inputRole")
        if not role_id:
            return apology("Please select Role", 400)

        # Get role associated with role id
        new_role = db.execute("SELECT * FROM roles WHERE role_id = ?", role_id)
        role = new_role[0]['role']

        
        # Query database for username
        rows = db.execute(
            "SELECT * FROM users WHERE username = ?", username
        )
        
        # Ensure username doesn't already exist
        if len(rows) != 0:
            return apology("Username already exists", 400)
        
        # Query database for email
        row = db.execute(
            "SELECT * FROM users WHERE email = ?", email
        )
        # Ensure email doesn't already exist
        if len(row) != 0:
            return apology("Email already exists", 400)
        
        # Hash password
        hash = generate_password_hash(password, method='pbkdf2:sha256:600000', salt_length=16)

        # Store new user info into users database
        db.execute("""INSERT INTO users (first_name, last_name, email, username, hash, role_id, role) VALUES (?, ?, ?, ?, ?, ?, ?)""", 
                   first_name, last_name, email, username, hash, role_id, role)

        # Get new user ID and Register the user
        user_rows = db.execute("SELECT * FROM users WHERE username = ?", username)
        username_id = user_rows[0]['id']
        
        db.execute("INSERT INTO registration (registration_user_id) VALUES (?)", username_id)

        # Check if user has profile
        row = db.execute("SELECT * FROM user_profiles WHERE profile_id = ?", username_id)
        has_profile = len(row)
        
        #if new profile
        if has_profile == 0:
            # Create new user profile
            db.execute("INSERT INTO user_profiles (profile_id, phone_number) VALUES (?, ?)", username_id, phone_number)

        else:    
            return apology("Profile already exists", 400)

        # If a student, automatically enroll them into their new program
        role_id_int = int(role_id)
        if role_id_int > 3:

            # Retrieve Program ID based on User Role
            id = db.execute("SELECT program_id FROM programs WHERE program = ?", role)
            program_id = id[0]['program_id']

            # Update user profile with program_id
            db.execute("UPDATE user_profiles SET user_program_id = ? WHERE profile_id = ?", program_id, username_id)
            
            # Enroll user into program
            db.execute("INSERT INTO program_enrollment (enrollment_user_id, enrollment_program_id) VALUES (?, ?)", username_id, program_id)

        return redirect("/users")
    
    else:
        return apology("You do not have permission to access this page", 400)


@app.route("/delete_user", methods=["POST"])
@login_required
def delete_user():

     # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Delete person from users, profile, registration and add to deleted users list
        id = request.form.get("delete_user_id")
        if id:

            # Check if user is an instructor, request instructor change first   
            instructor_check = db.execute("SELECT course_id, course_title FROM courses WHERE instructor_id = ?", id)
            instructor = len(instructor_check)
            if instructor:
                instructor_course = instructor_check[0]['course_title']
                return apology("Please update course instructor first", instructor_course)

            db.execute("DELETE FROM users WHERE id = ?", id)
            
        return redirect("/users")
    
    else:
        return apology("You do not have permission to access this page", 400)

# """ ----------------------- USERS END ----------------------- ""


# """ ----------------------- COURSES START ----------------------- ""

@app.route("/courses", methods=["GET", "POST"])
@login_required
def courses():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for edit button template
        flag = True

        # Display the courses in the database 
        rows = db.execute("SELECT * FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN users ON users.id = courses.instructor_id")

        # print(f"....COURSES: {rows}")

        return render_template("courses.html", courses=rows, no_edit=flag)
    
    else:
        return apology("You do not have permission to access this page", 400)
    
    

@app.route("/course_edit", methods=["POST"])
@login_required
def course_edit():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for course edit button
        flag = True

        # Select Course to edit
        id = request.form.get("course_edit_id")
        if id:

            rows = db.execute(
                """SELECT * FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN course_structure 
                ON courses.structure_id = course_structure.course_structure_id 
                JOIN languages ON courses.language_id = languages.language_id 
                JOIN users ON courses.instructor_id = users.id WHERE courses.course_id = ?""", id)
            
            # Select all fields to populate in form-select types
            levels = db.execute("SELECT * FROM levels")
            structures = db.execute("SELECT * FROM course_structure")
            languages = db.execute("SELECT * FROM languages")
            instructors = db.execute("SELECT * FROM users WHERE users.role_id = ? OR users.role_id = ?", 1, 2)
                
            return render_template("courses.html", yes_edit=flag, courses=rows, levels=levels, structures=structures, 
                                   languages=languages, instructors=instructors, edit_course_id=id)
        
        else:
            return apology("Course not found, Please define course id", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/course_edit_confirm", methods=["POST"])
@login_required
def course_edit_confirm():

     # Ensure valid admin user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Select Course to edit
        id = request.form.get("course_edit_id")

        if id:
            # Verify and upload picture first
            file = request.files.get('coursePicUpload')

            # If upload, picture is named secure filename
            if file and file.filename != '':
                picture = upload_img(file, app.config['ALLOWED_IMAGE_EXTENSIONS'])
                if not picture or picture == 'None':
                    return apology("File upload failed or file is not allowed", 400)
            
            else:
                # If no upload, pic is set to current or default pic
                picture = request.form.get("currentPic")
                # print(f"...Picture: {picture}")
                if not picture:
                    picture = 'course-img.png'

            # Verify and upload Published
            published = request.form.get("inputPublished")
            if published:
                published = int(published)

            # Verify start and end 
            start = request.form.get("inputStart")
            end = request.form.get("inputEnd")

            # Get new values for each variable in courses (Ensure foreign keys have value=id)
            title = request.form.get("inputTitle")
            subtitle = request.form.get("inputSubtitle")
            overview = request.form.get("inputOverview")
            capacity = request.form.get("inputCapacity")
            level = request.form.get("inputLevel")
            curriculum = request.form.get("inputCurriculum")
            prerequisite = request.form.get("inputPrerequisite")
            structure = request.form.get("inputStructure")
            language = request.form.get("inputLanguage")
            instructor = request.form.get("inputInstructor")
            
            # Ensure video is embed link
            video = request.form.get("inputVideo")
            if video and video != 'None':
                if 'https://www.youtube.com/embed/' not in video:
                    return apology("Youtube link must be from Embed link", 400)
                elif '"' in video:
                    return apology("Youtube link cannot include quotations", 400)
            
            #  Update each field in course
            db.execute("""
                       UPDATE courses SET 
                       (course_picture, course_title, course_subtitle, course_overview, course_capacity, level_id, course_curriculum, 
                       course_prerequisites, course_video, course_start, course_end, course_published, structure_id, language_id, instructor_id) 
                       = (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                       WHERE course_id = ?""", picture, title, subtitle, overview, capacity, level, curriculum, prerequisite, video, start, 
                       end, published, structure, language, instructor, id)

            # return render_template("courses.html", courses=rows, no_edit=flag)
            return redirect("/courses")
        
        else:
            return apology("Course not found, Please define course id", 400)
        
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/add_course", methods=["POST"])
@login_required
def add_course():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for add button template
        flag = True

        # Select all fields to populate in form-select types
        levels = db.execute("SELECT * FROM levels")
        structures = db.execute("SELECT * FROM course_structure")
        languages = db.execute("SELECT * FROM languages")
        instructors = db.execute("SELECT * FROM users WHERE users.role_id = ? OR users.role_id = ?", 1, 2)

        return render_template("courses.html", yes_add=flag, levels=levels, structures=structures, languages=languages, instructors=instructors)
    
    else:
        return apology("You do not have permission to access this page", 400)
    
    
@app.route("/course_add_confirm", methods=["POST"])
@login_required
def course_add_confirm():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Get values for each variable in courses (Ensure foreign keys have value=id)
        # Required
        title = request.form.get("inputTitle")
        if not title:
            return apology("Please enter Title", 400)
        subtitle = request.form.get("inputSubtitle")
        if not subtitle:
            return apology("Please enter Subtitle", 400)
        overview = request.form.get("inputOverview")
        if not overview:
            return apology("Please enter Overview", 400)
        capacity = request.form.get("inputCapacity")
        if not capacity:
            return apology("Please enter Capacity", 400)
        # Required Foreign
        level_id = request.form.get("inputLevel")
        if not level_id or level_id == '* Select Level':
            return apology("Please select Level", 400)
        
        structure_id = request.form.get("inputStructure")
        if not structure_id or structure_id == '* Select Structure':
            return apology("Please select Structure", 400)
        
        language_id = request.form.get("inputLanguage")
        if not language_id or language_id == '* Select Language':
            return apology("Please select Language", 400)
        
        instructor_id = request.form.get("inputInstructor")
        if not instructor_id or instructor_id == '* Select Instructor':
            return apology("Please select Instructor", 400)
        
        # Not Required
        curriculum = request.form.get("inputCurriculum")
        prerequisite = request.form.get("inputPrerequisite")

        # Ensure video is embed link
        video = request.form.get("inputVideo")
        if video:
            if 'https://www.youtube.com/embed/' not in video:
                return apology("Youtube link must be from Embed link", 400)
            elif '"' in video:
                return apology("Youtube link cannot include quotations", 400)

        # Verify and upload picture
        file = request.files.get('coursePicture')

        # print(f"...File: {file}")
        if file and file.filename != '':
            picture = upload_img(file, app.config['ALLOWED_IMAGE_EXTENSIONS'])
            
            if not picture:
                return apology("File upload failed or file is not allowed", 400)
        else:
            picture = 'course-img.png'

        # Verify start and end 
        start = request.form.get("inputStart")
        end = request.form.get("inputEnd")

        #  Update each field in course
        db.execute("""
                    INSERT INTO courses 
                   (course_picture, course_title, course_subtitle, course_overview, course_capacity, level_id, course_curriculum, course_prerequisites, 
                   course_video, course_start, course_end, structure_id, language_id, instructor_id) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
                    picture, title, subtitle, overview, capacity, level_id, curriculum, prerequisite, video, start, end, structure_id, language_id, instructor_id)

        return redirect("/courses")
    
    else:
        return apology("You do not have permission to add a course", 400)


@app.route("/delete_course", methods=["POST"])
@login_required
def delete_course():


    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Delete course
        id = request.form.get("delete_course_id")
        if id:
            
            db.execute("DELETE FROM courses WHERE course_id = ?", id)

        return redirect("/courses")
    
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/course_translate", methods=["POST"])
@login_required
def course_translate():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for course edit translation button
        flag = True

        # Select Course to edit
        id = request.form.get("course_edit_id")
        if id:

            # Display the course info from the database 
            rows = db.execute(
                """SELECT course_id, course_title, course_subtitle, course_overview, course_picture, course_video, 
                course_curriculum, course_prerequisites
                FROM courses WHERE course_id = ?""", id)
            # print(f"...COURSE TRANSLATE - ROWS:{rows}")
            
            # Select all fields to populate in form-select types
            languages = db.execute("SELECT language_id, language FROM languages")
            # print(f"...PROGRAM TRANSLATE  - Languages:{language}")

            # Check if current translations to show
            translations = db.execute(
                """SELECT * FROM course_translations WHERE ct_course_id = ?""", id)

            if translations:
                # print(f"...PROGRAM TRANSLATE  - Translations:{translations}")

                # Get translation id
                ct_id = translations[0]['ct_id']
            else:
                # Insert into translations and get info
                db.execute("INSERT INTO course_translations (ct_course_id) VALUES (?)", id)
                translations = db.execute("SELECT * FROM course_translations WHERE ct_course_id = ?", id)
                ct_id = translations[0]['ct_id']

            return render_template("courses.html", yes_edit_translate=flag, courses=rows, 
                                   translations=translations, languages=languages, edit_course_id=id, ct_id=ct_id)
        
        else:
            return apology("Course not found, Please define course id", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/course_translate_confirm", methods=["POST"])
@login_required
def course_translate_confirm():

     # Ensure valid admin user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Select Program
        id = request.form.get("course_edit_id")
        # Select Translation
        ct_id = request.form.get("course_translation_id")

        if id:
            # Verify and upload picture first
            file = request.files.get('ctPicUpload')
            # print(f"...File: {file}")

            # If upload, picture is named secure filename
            if file and file.filename != '':
                picture = upload_img(file, app.config['ALLOWED_IMAGE_EXTENSIONS'])
                # print(f"...PROGRAM EDIT CONFIRM - Picture: {picture}")
                if not picture:
                    return apology("File upload failed or file is not allowed", 400)
            else:
                # If no upload, pic is set to current or default pic
                picture = request.form.get("currentCTPic")
                # print(f"...PROGRAM EDIT CONFIRM - Picture (no upload): {picture}")
                if not picture or picture == 'None':
                    picture = 'course-img.png'
                    # print(f"...PROGRAM EDIT CONFIRM - Picture (default): {picture}")

            # Get new values for each variable in program (Ensure foreign keys have value=id)
            title = request.form.get("inputCTTitle")
            subtitle = request.form.get("inputCTSubtitle")
            overview = request.form.get("inputCTOverview")
            curriculum = request.form.get("inputCTCurriculum")
            prerequisite = request.form.get("inputCTPrerequisite")
            language = request.form.get("inputCTLanguage")
            # print(f"...PROGRAM TRANSLATE CONFIRM - Language: {language}")
            
            # Ensure video is embed link
            video = request.form.get("inputCTVideo")
            if video and video != 'None':
                if 'https://www.youtube.com/embed/' not in video:
                    return apology("Youtube link must be from Embed link", 400)
                elif '"' in video:
                    return apology("Youtube link cannot include quotations", 400)
            
            #  Update each field in course translation
            db.execute(
                """UPDATE course_translations SET (ct_title, ct_subtitle, ct_overview, ct_picture, ct_video, 
                ct_curriculum, ct_prerequisites, ct_language_id) = 
                (?, ?, ?, ?, ?, ?, ?, ?) WHERE ct_course_id = ?""", 
                title, subtitle, overview, picture, video, curriculum, prerequisite, language, id)

            # return render_template("courses.html", courses=rows, no_edit=flag)
            return redirect("/courses")
        
        else:
            return apology("Course not found, Please define course id", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

# Fetch translation based on user's language
def get_course(course_id):
    user_lang = str(get_locale())

    if user_lang == 'fr':
        lang = 2
    else:
        lang = 1

    # Fetch the default course data
    course = db.execute("SELECT * FROM courses WHERE course_id = ?", course_id)[0]

    # Fetch the translation based on the user's language
    translation = db.execute("SELECT * FROM course_translations WHERE ct_course_id = ? AND ct_language_id = ?", course_id, lang)
    if translation:
        translation = translation[0]
        return {
            'course_title': translation['ct_title'], # Translatable fields
            'course_subtitle': translation['ct_subtitle'],
            'course_overview': translation['ct_overview'],
            'course_picture': translation['ct_picture'],
            'course_video': translation['ct_video'],
            'course_curriculum': translation['ct_curriculum'],
            'course_prerequisites': translation['ct_prerequisites'],
            'course_start': course['course_start'], # Non-Translatable fields
            'course_end': course['course_end'],
            'course_capacity': course['course_capacity'],
            'course_published': course['course_published'],
            'structure_id': course['structure_id'],
            'language_id': course['language_id'],
            'instructor_id': course['instructor_id'],
            'level_id': course['level_id']

        }
    else:
        return {
            'course_title': course['course_title'],
            'course_subtitle': course['course_subtitle'],
            'course_overview': course['course_overview'],
            'course_picture': course['course_picture'],
            'course_video': course['course_video'],
            'course_curriculum': course['course_curriculum'],
            'course_prerequisites': course['course_prerequisites'],
            'course_start': course['course_start'],
            'course_end': course['course_end'],
            'course_capacity': course['course_capacity'],
            'course_published': course['course_published'],
            'structure_id': course['structure_id'],
            'language_id': course['language_id'],
            'instructor_id': course['instructor_id'],
            'level_id': course['level_id']
        }
    

@app.route("/pictures")
@login_required
def pictures():

    # Ensure valid user
    users_id = session["user_id"]
    rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = rows[0]['role_id']

    if user_role == 1:

        files = os.listdir(app.config['UPLOAD_IMG_DIRECTORY'])
        images = []

        for file in files:
            extension = os.path.splitext(file)[1].lower()
            if extension in app.config['ALLOWED_IMAGE_EXTENSIONS']:
                images.append(file)

        return render_template("pictures.html", images=images)
    
    else:
        # TODO: change eventually
        return redirect('login.html')
    

@app.route("/published_courses", methods=["GET", "POST"])
@login_required
def published_courses():
    
    # Ensure valid user
    users_id = session["user_id"]
    rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = rows[0]['role_id']

    if user_role:

        # Display the courses in the database 
        rows = db.execute("SELECT * FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN users ON users.id = courses.instructor_id")

        return render_template('published_courses.html', courses=rows)
    
    else:
        return redirect('/login')


@app.route("/course_records", methods=["GET", "POST"])
@login_required
def course_records():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:
        
        # Flag for view template
        flag = True
        
        # Course enrollment information
        enrolled_info = db.execute(
            """SELECT cg_course_id, course_title, course_capacity, language_id, course_started, 
            COUNT(DISTINCT cg_user_id) AS user_count,
            COUNT(DISTINCT CASE WHEN users.role_id > 3 THEN cg_user_id END) AS student_count FROM course_grades 
            JOIN courses ON courses.course_id = course_grades.cg_course_id JOIN users ON courses.instructor_id = users.id GROUP BY cg_course_id""")
        
        # Course completion information
        completed_info = db.execute(
            """SELECT cg_course_id, course_title, language_id, course_completed, course_completed_datetime, 
            COUNT(DISTINCT cg_user_id) FROM course_grades JOIN courses ON courses.course_id = course_grades.cg_course_id 
            WHERE course_completed = ? GROUP BY cg_course_id""", 1)
        
        if completed_info:
            return render_template("course_records.html", courses=enrolled_info, completed=completed_info)

        # Regular return
        return render_template("course_records.html", courses=enrolled_info, no_view=flag)
    
    else:
        return apology("You do not have permission to access this page", 400)
    

# Course Detailed Record
@app.route('/course_detail_record/<int:course_id>', methods=["POST"])
@login_required
def course_detail_record(course_id):

    # Check if Admin User
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role =  user_rows[0]['role_id']

    if user_role == 1:

        # Course Information
        info = db.execute(
            """SELECT cg_course_id, cg_user_id, course_started, course_started_datetime, course_completed, course_completed_datetime, course_grade, 
            course_title, course_capacity, language_id, 
            id, first_name, last_name, username, role
            FROM course_grades JOIN courses ON course_grades.cg_course_id = courses.course_id 
            JOIN users ON course_grades.cg_user_id = users.id WHERE cg_course_id = ? ORDER BY cg_user_id""", course_id)
        
        # print(f"....COURSE DETAIL RECORD - COURSE: {info}")
        
        if info:
            course_title = info[0]['course_title']

        return render_template('course_detail_record.html', courses=info, course_title=course_title)

    else:
        return redirect('/index')


@app.route('/course_detail/<int:course_id>', methods=["POST"])
@login_required
def course_detail(course_id):

    # Check if Admin User
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role =  user_rows[0]['role_id']

    if user_role == 1:
        flag = True
    
    else:
        flag = False

    # Check user language:
    user_lang = str(get_locale())
    if user_lang == 'fr':
        lang = 2
    else:
        lang = 1

    # Check for first section:
    section_row = db.execute(
        """SELECT section_id FROM sections WHERE section_course_id = ? ORDER BY section_number ASC""", course_id)

    if section_row:
        first_section = section_row[0]['section_id']
        next_flag = True

    else:
        next_flag = False
        return apology("Cannot view course without a section, please create a section first.", 400)
        # print(f"....COURSE DETAIL - Next Section: {first_section}")

    # Course detail for page
    course = get_course(course_id)

    # Instructor Details
    instructor = db.execute("""SELECT first_name, last_name FROM courses JOIN users ON courses.instructor_id = users.id where course_id = ?""", course_id)[0]

    return render_template('course_detail.html', course=course, users=flag, first_section=first_section, first_session_exits=next_flag, 
                           lang=lang, instructor=instructor)




# """ ----------------------- COURSES END ----------------------- ""


# """ ----------------------- SECTION START ----------------------- ""

@app.route("/sections", methods=["GET", "POST"])
@login_required
def sections():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    user_row = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_row[0]['role_id']

    if user_role == 1:

        # Flag for edit button template
        flag = True

        # Get course id
        course_id = request.form.get("section_course_id")

        # print(f"..COURSE ID: {course_id}")

        if course_id:
            # Get course title:
            course_info = db.execute(
                """SELECT course_title FROM courses WHERE course_id = ?""", course_id)

            # print(f"....Lessons - lesson_course: {lesson_course}")

            course_title = course_info[0]['course_title']

            # Check if this is first section added to course
            existing_sections = db.execute("SELECT section_id FROM courses JOIN sections ON courses.course_id = sections.section_course_id WHERE course_id = ?", course_id)

            # debug course number issue
            # print(f"....Existing Sections: {existing_sections}")
            
            if not existing_sections:
                
                rows = db.execute(
                """SELECT * FROM courses WHERE course_id = ?""", course_id)
                # print(f"....New Section: {rows}")

            else:
                # Display the courses and sections in the database 
                rows = db.execute(
                    """SELECT * FROM sections WHERE section_course_id = ? ORDER BY sections.section_number ASC""", course_id)

            return render_template("sections.html", courses=rows, no_edit=flag, course_id=course_id, course_title=course_title)
        
        else:
            return apology("Section course id not selected", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/add_section", methods=["POST"])
@login_required
def add_section():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role =  user_rows[0]['role_id']

    if user_role == 1:
        
        # Flag for add button template
        flag = True

        # Select Course to edit
        id = request.form.get("section_course_id")
        if id:

            course_info = db.execute( """SELECT course_title FROM courses WHERE course_id = ?""", id)

            # print(f"....Lessons - lesson_course: {lesson_course}")

            course_title = course_info[0]['course_title']

            return render_template("sections.html", yes_add=flag, course_id=id, course_title=course_title)
        
        else:
            return apology("Course ID not found", 400)
    
    else:
        return apology("You do not have permission to access this page", 400)
    
@app.route("/section_add_confirm", methods=["POST"])
@login_required
def section_add_confirm():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role =  user_rows[0]['role_id']

    if user_role == 1:

        id = request.form.get("section_course_id")
        if id:

            # Flag for add button template
            flag = True

            # Get values for each variable in section (Ensure foreign keys have value=id)
            # Required
            title = request.form.get("sectionTitle")
            if not title:
                return apology("Please enter Title", 400)
            
            subtitle = request.form.get("sectionSubtitle")
            if not subtitle:
                return apology("Please enter Subtitle", 400)
            
            overview = request.form.get("sectionOverview")
            
            # Increment Section Number
            sec = db.execute("Select section_number FROM sections WHERE section_course_id = ? ORDER BY section_number ASC", id)
            if sec:

                prev_sec = sec[-1]['section_number']
                # print(f"..PREV SEC NUM: {prev_sec}")
                
            else:
                prev_sec = 0

            section_number = prev_sec + 1

            # Ensure video is embed link
            video = request.form.get("sectionVideo")
            if video:
                if 'https://www.youtube.com/embed/' not in video:
                    return apology("Youtube link must be from Embed link", 400)
                elif '"' in video:
                    return apology("Youtube link cannot include quotations", 400)

            # Verify and upload picture
            file = request.files.get('sectionPicture')
            # print(f"...File: {file}")
            if file and file.filename != '':
                picture = upload_img(file, app.config['ALLOWED_IMAGE_EXTENSIONS'])
                
                if not picture:
                    return apology("File upload failed or file is not allowed", 400)
            else:
                picture = 'course-img.png'

            db.execute("""
                        INSERT INTO sections 
                    (section_title, section_subtitle, section_overview, section_picture, section_video, section_number, section_course_id) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)""", title, subtitle, overview, picture, video, section_number, id 
                    )
            
            rows = db.execute(
                """SELECT * FROM sections WHERE section_course_id = ? ORDER BY section_number ASC""", id)
            
            course_info = db.execute( """SELECT course_title FROM courses WHERE course_id = ?""", id)

            course_title = course_info[0]['course_title']

            return render_template("sections.html", no_edit=flag, courses=rows, course_id=id, course_title=course_title )
    
    else:
        return apology("You do not have permission to add a section", 400)
    

@app.route("/section_edit", methods=["POST"])
@login_required
def section_edit():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for course edit button
        flag = True

        # Select Course to edit
        id = request.form.get("section_edit_id")

        if id:

            rows = db.execute(
                """SELECT * FROM sections JOIN courses ON sections.section_course_id = courses.course_id WHERE section_id = ?""", id)
            
            course_edit = rows[0]['section_course_id']
            course_title = rows[0]['course_title']
                
            return render_template("sections.html", yes_edit=flag, courses=rows, section_edit_id=id, course_edit=course_edit, course_title=course_title)
        
        else:
            return apology("No Sections in this course, please add a section", 400)
    else:
        return apology("You do not have permission to access this page", 400)


@app.route("/section_edit_confirm", methods=["POST"])
@login_required
def section_edit_confirm():

     # Ensure valid admin user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

         # Flag for course edit button
        flag = True

        # Select Section to edit
        course_id = request.form.get("course_edit_id")

        id = request.form.get("section_edit_id")

        if id:
            # Verify and upload picture first
            file = request.files.get('sectionNewPic')

            # If upload, picture is named secure filename
            if file and file.filename != '':
                picture = upload_img(file, app.config['ALLOWED_IMAGE_EXTENSIONS'])
                if not picture:
                    return apology("File upload failed or file is not allowed", 400)
            
            else:
                # If no upload, pic is set to current or default pic
                picture = request.form.get("sectionPicture")
                if not picture or picture == 'None':
                    picture = 'course-img.png'

            # Verify and upload Published
            published = request.form.get("sectionPublish")
            if published:
                published = int(published)

            # Get new values for each variable in courses (Ensure foreign keys have value=id)
            title = request.form.get("sectionTitle")
            subtitle = request.form.get("sectionSubtitle")
            overview = request.form.get("sectionOverview")

            # Check Section
            sec = db.execute("Select section_number FROM sections WHERE section_course_id = ? ORDER BY section_number ASC", course_id)
            if sec:

                section_number = request.form.get("inputSectionNumber")
                if not section_number:
                    return apology("Please add a section number", 400)

            # Ensure video is embed link
            video = request.form.get("sectionVideo")
            if video:
                if 'https://www.youtube.com/embed/' not in video:
                    return apology("Youtube link must be from Embed link", 400)
                elif '"' in video:
                    return apology("Youtube link cannot include quotations", 400)
            
            #  Update each field in course
            db.execute("""
                       UPDATE sections SET 
                       (section_title, section_subtitle, section_overview, section_picture, section_video, section_published, section_number) 
                       = (?, ?, ?, ?, ?, ?, ?) 
                       WHERE section_id = ?""", title, subtitle, overview, picture, video, published, section_number, id)
            
            # Query the courses in the database 
            rows = db.execute(
                """SELECT  * FROM courses JOIN sections ON courses.course_id = sections.section_course_id 
                WHERE section_course_id = ? ORDER BY section_number ASC""", course_id)

            course_title = rows[0]['course_title']

            # return render_template("courses.html", courses=rows, no_edit=flag)
            return render_template("sections.html", courses=rows, no_edit=flag, course_id=course_id, course_title=course_title)
        
        else:
            return apology("Course not found, Please define course id", 400)
        
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/delete_section", methods=["POST"])
@login_required
def delete_section():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Delete Section
        id = request.form.get("delete_section_id")
        if id:
            
            db.execute("DELETE FROM sections WHERE section_id = ?", id)

            # Flag for edit button template
            flag = True
            # Course Id to return to
            course_id = request.form.get("course_edit_id")

            # Check if sections left in course
            existing_sections = db.execute(
                """SELECT section_id FROM courses 
                JOIN sections ON courses.course_id = sections.section_course_id WHERE course_id = ?""", course_id)
            
            if not existing_sections:
                
                rows = db.execute(
                """SELECT * FROM courses WHERE course_id = ?""", course_id)
                # print(f"....Remaining Section: {rows}")
                
            else:
                # Display the courses and sections in the database 
                rows = db.execute(
                    """SELECT * FROM courses JOIN sections ON courses.course_id = sections.section_course_id 
                    WHERE course_id = ? ORDER BY sections.section_number ASC""", course_id)
                
            course_title = rows[0]['course_title']

            # print(f"....DELETE SECTION rows: {rows}")
        return render_template("sections.html", courses=rows, course_id=course_id, no_edit=flag, course_title=course_title)
    
    else:
        return apology("You do not have permission to access this page", 400)


@app.route("/section_translate", methods=["POST"])
@login_required
def section_translate():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for section edit translation button
        flag = True

        # Select section to edit
        id = request.form.get("section_edit_id")
        if id:

            if id:
                row_info = db.execute(
                """SELECT section_course_id, course_title FROM sections 
                JOIN courses ON sections.section_course_id = courses.course_id WHERE section_id = ?""", id)
                if row_info:
                    course_edit = row_info[0]['section_course_id']
                    course_title = row_info[0]['course_title']

            # Display the section info from the database 
            rows = db.execute(
                """SELECT section_id, section_title, section_subtitle, section_overview, section_picture, section_video
                FROM sections WHERE section_id = ?""", id)
            # print(f"...section TRANSLATE - ROWS:{rows}")
            
            # Select all fields to populate in form-select types
            languages = db.execute("SELECT language_id, language FROM languages")
            # print(f"...section TRANSLATE  - Languages:{language}")

            # Check if current translations to show
            translations = db.execute(
                """SELECT * FROM  section_translations WHERE st_section_id = ?""", id)

            if translations:
                # print(f"...section TRANSLATE  - Translations:{translations}")

                # Get translation id
                st_id = translations[0]['st_id']
            else:
                # Insert into translations and get info
                db.execute("INSERT INTO section_translations (st_section_id) VALUES (?)", id)
                translations = db.execute("SELECT * FROM section_translations WHERE st_section_id = ?", id)
                st_id = translations[0]['st_id']

            return render_template("sections.html", yes_edit_translate=flag, courses=rows, course_edit=course_edit, course_title=course_title,
                                   translations=translations, languages=languages, edit_section_id=id, st_id=st_id)
        
        else:
            return apology("Course not found, Please define course id", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/section_translate_confirm", methods=["POST"])
@login_required
def section_translate_confirm():

     # Ensure valid admin user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Select Program
        id = request.form.get("section_edit_id")
        # Select Translation
        st_id = request.form.get("section_translation_id")

        if id:

            # Get course id and set flag
            course_id = request.form.get("section_course_id")
            flag = True

            # Verify and upload picture first
            file = request.files.get('stPicUpload')
            # print(f"...File: {file}")

            # If upload, picture is named secure filename
            if file and file.filename != '':
                picture = upload_img(file, app.config['ALLOWED_IMAGE_EXTENSIONS'])
                # print(f"...SECTION EDIT CONFIRM - Picture: {picture}")
                if not picture:
                    return apology("File upload failed or file is not allowed", 400)
            else:
                # If no upload, pic is set to current or default pic
                picture = request.form.get("currentSTPic")
                # print(f"...SECTION EDIT CONFIRM - Picture (no upload): {picture}")
                if not picture or picture == 'None':
                    picture = 'course-img.png'
                    # print(f"...SECTION EDIT CONFIRM - Picture (default): {picture}")

            # Get new values for each variable in program (Ensure foreign keys have value=id)
            title = request.form.get("inputSTTitle")
            subtitle = request.form.get("inputSTSubtitle")
            overview = request.form.get("inputSTOverview")
            language = request.form.get("inputSTLanguage")
            # print(f"...SECTION TRANSLATE CONFIRM - Language: {language}")
            
            # Ensure video is embed link
            video = request.form.get("inputSTVideo")
            if video and video != 'None':
                if 'https://www.youtube.com/embed/' not in video:
                    return apology("Youtube link must be from Embed link", 400)
                elif '"' in video:
                    return apology("Youtube link cannot include quotations", 400)
            
            # Update each field in section translation
            db.execute(
                """UPDATE section_translations SET (st_title, st_subtitle, st_overview, st_picture, st_video, st_language_id) = 
                (?, ?, ?, ?, ?, ?) WHERE st_section_id = ?""", 
                title, subtitle, overview, picture, video, language, id)
            
            # Query the courses in the database 
            rows = db.execute(
                """SELECT  * FROM courses JOIN sections ON courses.course_id = sections.section_course_id 
                WHERE section_course_id = ? ORDER BY section_number ASC""", course_id)

            course_title = rows[0]['course_title']

            return render_template("sections.html", courses=rows, no_edit=flag, course_id=course_id, course_title=course_title)
            # return redirect("/sections")
        else:
            return apology("Course not found, Please define course id", 400)
    else:
        return apology("You do not have permission to access this page", 400)


# Fetch translation based on user's language
def get_section(section_id):
    user_lang = str(get_locale())

    if user_lang == 'fr':
        lang = 2
    else:
        lang = 1

    # Fetch the default section data
    section = db.execute("SELECT * FROM sections WHERE section_id = ?", section_id)[0]

    # Fetch the translation based on the user's language
    translation = db.execute("SELECT * FROM section_translations WHERE st_section_id = ? AND st_language_id = ?", section_id, lang)
    if translation:
        translation = translation[0]
        return {
            'section_title': translation['st_title'], # Translatable fields
            'section_subtitle': translation['st_subtitle'],
            'section_overview': translation['st_overview'],
            'section_picture': translation['st_picture'],
            'section_video': translation['st_video'],
            'section_number': section['section_number'], # Non-Translatable fields
            'section_published': section['section_published'], 
            'section_course_id': section['section_course_id'],
            'section_id': section['section_id']
        }
    else:
        return {
            'section_title': section['section_title'], 
            'section_subtitle': section['section_subtitle'],
            'section_overview': section['section_overview'],
            'section_picture': section['section_picture'],
            'section_video': section['section_video'],
            'section_number': section['section_number'],
            'section_published': section['section_published'], 
            'section_course_id': section['section_course_id'],
            'section_id': section['section_id']
        }
    

@app.route('/section_detail/<int:section_id>', methods=["POST"])
@login_required
def section_detail(section_id):

    # Check if Admin User
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role =  user_rows[0]['role_id']
    

    # Flag to show admin nav
    if user_role == 1:
        flag = True
    
    else:
        flag = False

    # Section info
    section= get_section(section_id)
    if not section:
      return apology("Section not found", 404)

    # Get course title
    course_row = db.execute(
        """SELECT course_title, course_id FROM courses JOIN sections ON courses.course_id = sections.section_course_id 
        WHERE sections.section_id = ?""", section_id
    )

    if course_row:
        # course_title = course_row [0]['course_title']
        course_id = course_row[0]['course_id']

        # Get course translation for title
        course = get_course(course_id)
        course_title = course['course_title']
        # print(f"....SECTION DETAIL - Course Title: {course_title}")

        # Check if User is already enrolled into course and section grades
        course_grades = db.execute("Select course_started FROM course_grades WHERE cg_course_id = ? AND cg_user_id = ?", course_id, users_id)
        section_grades = db.execute("SELECT section_started FROM section_grades WHERE sg_section_id = ? AND sg_user_id = ?", section_id, users_id)
    
        # If not enrolled, enroll user
        if not course_grades or course_grades == 0:
            db.execute(
                """INSERT INTO course_grades (course_started, cg_course_id, cg_user_id) VALUES (?, ?, ?)""", 1, course_id, users_id)
        if not section_grades or section_grades == 0:
            db.execute(
                """INSERT INTO section_grades (section_started, sg_section_id, sg_user_id) VALUES (?, ?, ?)""", 1, section_id, users_id)

    # Check for first lesson:
    lesson_row = db.execute(
        """SELECT lesson_id FROM lessons WHERE lesson_section_id = ? ORDER BY lesson_number ASC""", section_id)
    
    # print(f"....SECTION DETAIL - Lesson_row: {lesson_row}")

    if not lesson_row:
        return apology("No lessons in this section", 404)

    first_lesson = lesson_row[0]['lesson_id']

    next_flag = True

    # Flags:
    prev_lesson = False

    # Is this first section in course?
    info = db.execute("SELECT section_id, section_number FROM sections WHERE section_course_id = ? ORDER BY section_number", course_id)
    # print(f"....SECTION DETAIL - First: {info}")
    if info:
        first_section = info[0]['section_id']
        if first_section == section_id:
            prev_course = course_id
        else:
            prev_course = False

        # Check current section
        for index, sections in enumerate(info):
            current_section = info[index]['section_id']
            if current_section == section_id and current_section != first_section:
                prev_section = info[index - 1]['section_id']
                # print(f"....SECTION DETAIL - Prev Section ID: {prev_section}")

                # Check lessons in previous section
                list = db.execute("SELECT lesson_id, lesson_number FROM lessons WHERE lesson_section_id = ? ORDER BY lesson_number", prev_section)
                if list:
                    prev_lesson = list[-1]['lesson_id']
                    # print(f"....SECTION DETAIL - Prev Lesson: {prev_lesson}")
            
                # IF not first section, Update user's PREVIOUS lesson grades (completed, completed time, grade) AND PREVIOUS section grades (grade)
                update_lg_sg(prev_section, prev_lesson, users_id)
                # Mark prev section complete, complete datetime
                mark_sec_complete(prev_section, users_id)

    return render_template('section_detail.html', section=section, users=flag, next_lesson_exists=next_flag, first_lesson=first_lesson, 
                           course_title=course_title, prev_course=prev_course, prev_lesson=prev_lesson)

# """ ----------------------- SECTION END ----------------------- ""

# """ ----------------------- LESSON START ----------------------- ""

@app.route("/lessons", methods=["GET", "POST"])
@login_required
def lessons():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    user_row = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_row[0]['role_id']

    if user_role == 1:

        # Flag for edit button template
        flag = True

        # Get section id
        section_id = request.form.get("lesson_section_id")
        # print(f"..Lessons - Section ID: {section_id}")

        if section_id:
            # print(f"....COURSE ID: {course_id}")

            # Get course title:
            lesson_course = db.execute(
                """SELECT course_title, course_id, section_title FROM courses 
                JOIN sections ON courses.course_id = sections.section_course_id WHERE sections.section_id = ?""", section_id)
            # print(f"....Lessons - lesson_course: {lesson_course}")
            course_id = lesson_course[0]['course_id']
            course_title = lesson_course[0]['course_title']
            section_title = lesson_course[0]['section_title']

            # print(f"....Lessons - Course Title: {course_title}")
            # print(f"....Lessons - Section Title: {section_title}")

            # Check if this is first lesson added to course
            existing_lessons = db.execute(
                """SELECT lesson_id FROM lessons 
                JOIN sections on lessons.lesson_section_id = sections.section_id WHERE sections.section_id = ?""", section_id)
            # print(f"....Lessons - Existing Lessons: {existing_lessons}")
            
            if not existing_lessons:
                
                rows = db.execute(
                """SELECT * FROM sections WHERE section_id = ?""", section_id)
                # print(f"....New Lesson: {rows}")

            else:
                
                # GO through other areas and implement similar (reduced) database queries to only what is needed
                # Display the sections, and lessons in the database for this section
                rows = db.execute(
                    """SELECT * FROM lessons WHERE lesson_section_id = ? ORDER BY lessons.lesson_number ASC""", section_id)
                # print(f"....LESSON: {rows}")

            return render_template("lessons.html", no_edit=flag, lessons=rows, section_id=section_id, course_title=course_title, 
                                   course_id=course_id, section_title=section_title)
        
        else:
            return apology("Section id not selected", 400)
    
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/add_lesson", methods=["POST"])
@login_required
def add_lesson():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role =  user_rows[0]['role_id']

    if user_role == 1:
        
        # Flag for add button template
        flag = True

        # Select Section to edit
        id = request.form.get("lesson_section_id")
        # print(f"....Lesson Add - Section Add ID: {id}")
        if id:
            return render_template("lessons.html", yes_add=flag, section_id=id)
        else:
            return apology("Course ID not found", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/lesson_add_confirm", methods=["POST"])
@login_required
def lesson_add_confirm():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role =  user_rows[0]['role_id']

    if user_role == 1:

        id = request.form.get("lesson_section_id")

        if id:

            # Flag for add button template
            flag = True

            # Get values for each variable in section (Ensure foreign keys have value=id)
            # Required
            title = request.form.get("lessonTitle")
            if not title:
                return apology("Please enter Title", 400)
            
            subtitle = request.form.get("lessonSubtitle")
            overview = request.form.get("lessonOverview")
            link = request.form.get("lessonLink")
            text = request.form.get("lessonText")

            # Increment Section Number
            les = db.execute("Select lesson_number FROM lessons WHERE lesson_section_id = ? ORDER BY lesson_number ASC", id)
            if les:
                # print(f"..SEC: {sec}")
                prev_lesson = les[-1]['lesson_number']
                # print(f"..PREV SEC NUM: {prev_sec}")
            else:
                prev_lesson = 0

            lesson_number = prev_lesson + 1

            # Ensure video is embed link
            video = request.form.get("lessonVideo")
            if video:
                if 'https://www.youtube.com/embed/' not in video:
                    return apology("Youtube link must be from Embed link", 400)
                elif '"' in video:
                    return apology("Youtube link cannot include quotations", 400)
                

            # Verify and upload picture
            file = request.files.get('lessonPicture')
            # print(f"...File: {file}")
            if file and file.filename != '':
                picture = upload_img(file, app.config['ALLOWED_IMAGE_EXTENSIONS'])
                
                if not picture:
                    return apology("File upload failed or file is not allowed", 400)
            else:
                picture = 'course-img.png'

            db.execute(
                """INSERT INTO lessons 
                    (lesson_title, lesson_subtitle, lesson_overview, lesson_picture, lesson_video, lesson_number, lesson_link, lesson_text, lesson_section_id) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", title, subtitle, overview, picture, video, lesson_number, link, text, id 
                    )
            
            # Select all lesson information
            rows = db.execute(
                """SELECT * FROM lessons WHERE lesson_section_id = ? ORDER BY lesson_number ASC""", id)
            
            # Get course title:
            lesson_course = db.execute(
                """SELECT course_title, section_title FROM courses 
                JOIN sections ON courses.course_id = sections.section_course_id WHERE sections.section_id = ?""", id)
            # print(f"....Lessons - lesson_course: {lesson_course}")

            course_title = lesson_course[0]['course_title']
            section_title = lesson_course[0]['section_title']
            # print(f"...ROWS: {rows}")

            return render_template("lessons.html", no_edit=flag, lessons=rows, section_id=id, course_title=course_title, section_title=section_title)
    
    else:
        return apology("You do not have permission to add a lesson", 400)


@app.route("/lesson_edit", methods=["POST"])
@login_required
def lesson_edit():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for course edit button
        flag = True

        # Select Lesson to edit
        id = request.form.get("lesson_edit_id")
        # print(f"...LESSON EDIT - LESSON EDIT ID:{id}")

        if id:
            # Display Current Lesson information
            rows = db.execute(
                """SELECT * FROM lessons WHERE lessons.lesson_id = ?""", id)
            
            # Define section to return to on exit
            section_id = rows[0]['lesson_section_id']
                
            return render_template("lessons.html", yes_edit=flag, lessons=rows, section_id=section_id, lesson_edit_id=id)
        else:
            return apology("No Sections in this course, please add a section", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/lesson_edit_confirm", methods=["POST"])
@login_required
def lesson_edit_confirm():

     # Ensure valid admin user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

         # Flag for lesson edit button
        flag = True

        section_id = request.form.get("section_edit_id")
        # print(f"..LESSON EDIT CONFIRM - SECTION ID: {section_id}")

        # Ensure Lesson ID
        id = request.form.get("lesson_edit_id")
        # print(f"..LESSON EDIT CONFIRM - LESSON ID: {id}")

        if id:
            # Upload Picture after verifying
            file = request.files.get('lessonNewPic')
            # print(f"...File: {file}")

            # If upload, picture is named secure filename
            if file and file.filename != '':
                picture = upload_img(file, app.config['ALLOWED_IMAGE_EXTENSIONS'])
                # print(f"...Picture: {picture}")
                if not picture:
                    return apology("File upload failed or file is not allowed", 400)
            
            else:
                # If no upload, pic is set to current or default pic
                picture = request.form.get("lessonPicture")
                # print(f"...Picture: {picture}")
                if not picture or picture == 'None':
                    picture = 'course-img.png'

            # Upload PDF after verifying
            file = request.files.get('lessonNewPDF')
            # print(f"...LESSON EDIT CONFIRM - PDF: {file}")
            if file and file.filename != '':
                pdf = upload_doc(file, app.config['ALLOWED_DOC_EXTENSIONS'])
                
                if not pdf:
                    # flash('Document upload failed or file is not allowed.', 'warning')
                    return apology("Document upload failed or file is not allowed", 400)
            else:
                # If no upload, pdf is set to current or None
                pdf = request.form.get("lessonPDF")
                if not pdf:
                    pdf = ''
            
            # Link Placeholder for PDF
            pdf_placeholder = request.form.get("lessonPDFplaceholder")

            # Verify and upload Published
            published = request.form.get("lessonPublish")
            if published:
                published = int(published)

            # Get new values for each variable in courses (Ensure foreign keys have value=id)
            title = request.form.get("lessonTitle")
            subtitle = request.form.get("lessonSubtitle")
            overview = request.form.get("lessonOverview")
            text = request.form.get("lessonText")

            # Link (hyperlink)
            link = request.form.get("lessonLink")

            # Link Placeholder for hyperlink
            link_placeholder = request.form.get("lessonLinkPlaceholder")

            # Check Lesson
            les = db.execute("Select lesson_number FROM lessons WHERE lesson_section_id = ? ORDER BY lesson_number ASC", section_id)
            # print(f"..SEC: {les}")
            if les:
                # prev_les = les[-1]['lesson_number']
                # if not prev_les:
                #     prev_les = 0

                lesson_number = request.form.get("inputLessonNumber")
                if not lesson_number:
                    return apology("Please add a lesson number", 400)
                # print(f"..Lesson Number: {lesson_number}")
                # if prev_les > lesson_number:

            # Ensure video is embed link
            video = request.form.get("lessonVideo")
            if video:
                if 'https://www.youtube.com/embed/' not in video:
                    return apology("Youtube link must be from Embed link", 400)
                elif '"' in video:
                    return apology("Youtube link cannot include quotations", 400)
            
            #  Update each field in course lesson
            db.execute("""
                       UPDATE lessons SET (lesson_title, lesson_subtitle, lesson_overview, lesson_picture, lesson_pdf_placeholder, 
                       lesson_pdf, lesson_video, lesson_text, lesson_link_placeholder, lesson_link, 
                       lesson_number, lesson_published, lesson_section_id) = (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                       WHERE lesson_id = ?""", title, subtitle, overview, picture, pdf_placeholder, pdf, video, text, 
                       link_placeholder, link, lesson_number, published, section_id, id)
            
            
            # Query the lessons in the database 
            rows = db.execute(
                """SELECT * FROM lessons WHERE lesson_section_id = ? ORDER BY lessons.lesson_number ASC""", section_id)
            # print(f"....Lessons Edit Confirm- rows: {rows}")

            # Get course title:
            lesson_course = db.execute(
                """SELECT course_title, section_title FROM courses 
                JOIN sections ON courses.course_id = sections.section_course_id WHERE sections.section_id = ?""", section_id)
            # print(f"....Lessons Edit Confirm- lesson_course: {lesson_course}")

            course_title = lesson_course[0]['course_title']
            section_title = lesson_course[0]['section_title']

            return render_template("lessons.html", no_edit=flag, lessons=rows, section_id=section_id, course_title=course_title, section_title=section_title)
        
        else:
            return apology("Lesson not found, Please define lesson id", 400)
        
    else:
        return apology("You do not have permission to access this page", 400)


@app.route("/delete_lesson", methods=["POST"])
@login_required
def delete_lesson():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Delete Section
        id = request.form.get("delete_lesson_id")
        # print(f"..DELETE Section ID: {id}")
        if id:
            
            db.execute("DELETE FROM lessons WHERE lesson_id = ?", id)

            # Flag for edit button template
            flag = True

            # Section Id to return to
            section_id = request.form.get("section_edit_id")

            # Display the sections in the database 
            rows = db.execute(
                """SELECT * FROM sections WHERE section_id = ? ORDER BY sections.section_number ASC""", section_id)
            
            section_title = rows[0]['section_title']
            
            # Display course title and id
            course_info = db.execute(
                """SELECT course_title, course_id FROM courses JOIN sections on courses.course_id = sections.section_course_id 
             WHERE sections.section_id = ?""", section_id)
            
            if course_info:
                course_title = course_info[0]['course_title']
                course_id = course_info[0]['course_id']
            
            # Check if there are lessons
            existing_lessons = db.execute(
                """SELECT lesson_id FROM lessons 
                JOIN sections on lessons.lesson_section_id = sections.section_id WHERE sections.section_id = ?""", section_id)
            # print(f"....Lessons - Existing Lessons: {existing_lessons}")
            
            if not existing_lessons:
                rows = db.execute(
                """SELECT * FROM sections WHERE section_id = ?""", section_id)
                # print(f"....New Lesson: {rows}")

            else:
                # Display the sections, and lessons in the database for this section
                rows = db.execute(
                    """SELECT * FROM lessons WHERE lesson_section_id = ? ORDER BY lessons.lesson_number ASC""", section_id)
            
        return render_template("lessons.html", lessons=rows, no_edit=flag, course_id=course_id, course_title=course_title, 
                               section_title=section_title, section_id=section_id)
    
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/lesson_translate", methods=["POST"])
@login_required
def lesson_translate():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for lesson edit translation button
        flag = True

        # Select lesson to edit
        id = request.form.get("lesson_edit_id")
        if id:

             # Get Course and section info
            section_id = request.form.get("lesson_section_id")
            # section_title = 
            course_id = request.form.get("lesson_course_id")
            # course_title = 

            # Display the lesson info from the database 
            rows = db.execute(
                """SELECT * FROM lessons WHERE lesson_id = ?""", id)
            # print(f"...lesson TRANSLATE - ROWS:{rows}")
            
            # Select all fields to populate in form-select types
            languages = db.execute("SELECT language_id, language FROM languages")
            # print(f"..lesson TRANSLATE  - Languages:{language}")

            # Check if current translations to show
            translations = db.execute(
                """SELECT * FROM lesson_translations WHERE lt_lesson_id = ?""", id)

            if translations:
                # print(f"...lesson TRANSLATE  - Translations:{translations}")

                # Get translation id
                lt_id = translations[0]['lt_id']
            else:
                # Insert into translations and get info
                db.execute("INSERT INTO lesson_translations (lt_lesson_id) VALUES (?)", id)
                translations = db.execute("SELECT * FROM lesson_translations WHERE lt_lesson_id = ?", id)
                lt_id = translations[0]['lt_id']

            return render_template("lessons.html", yes_edit_translate=flag, lessons=rows, course_id=course_id, section_id=section_id,
                                   translations=translations, languages=languages, edit_lesson_id=id, lt_id=lt_id)
        else:
            return apology("Lesson not found, Please define lesson id", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/lesson_translate_confirm", methods=["POST"])
@login_required
def lesson_translate_confirm():

     # Ensure valid admin user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Select Lesson
        id = request.form.get("lesson_edit_id")
        # Select Translation
        lt_id = request.form.get("lesson_translation_id")

        if id:
            # Get course and section id, titles, and set flag
            course_id = request.form.get("lesson_course_id")
            section_id = request.form.get("lesson_section_id")
            flag = True
            info = db.execute(
                """SELECT course_title, section_title FROM courses JOIN sections ON courses.course_id = sections.section_course_id 
                WHERE section_id = ?""", section_id)
            if info:
                course_title = info[0]['course_title']
                section_title = info[0]['section_title']

            # Verify and upload picture first
            file = request.files.get('ltPicUpload')
            # print(f"...File: {file}")

            # If upload, picture is named secure filename
            if file and file.filename != '':
                picture = upload_img(file, app.config['ALLOWED_IMAGE_EXTENSIONS'])
                # print(f"...LESSON TRANSLATE CONFIRM - Picture: {picture}")
                if not picture:
                    return apology("File upload failed or file is not allowed", 400)
            else:
                # If no upload, pic is set to current or default pic
                picture = request.form.get("currentLTPic")
                # print(f"...LESSON TRANSLATE CONFIRM - Picture (no upload): {picture}")
                if not picture or picture == 'None':
                    picture = 'course-img.png'
                    # print(f"...LESSON TRANSLATE CONFIRM - Picture (default): {picture}")

            # Get new values for each variable in lesson (Ensure foreign keys have value=id)
            title = request.form.get("inputLTTitle")
            subtitle = request.form.get("inputLTSubtitle")
            overview = request.form.get("inputLTOverview")
            language = request.form.get("inputLTLanguage")
            text = request.form.get("inputLTText")
            link_placeholder = request.form.get("ltLinkPlaceholder")
            link = request.form.get("ltLink")
            pdf_placeholder = request.form.get("ltPDFplaceholder")

            # Upload PDF after verifying
            file = request.files.get('ltNewPDF')
            # print(f"...LESSON EDIT CONFIRM - PDF: {file}")
            if file and file.filename != '':
                pdf = upload_doc(file, app.config['ALLOWED_DOC_EXTENSIONS'])
                if not pdf:
                    # flash('Document upload failed or file is not allowed.', 'warning')
                    return apology("Document upload failed or file is not allowed", 400)
            else:
                # If no upload, pdf is set to current or None
                pdf = request.form.get("ltPDF")
                if not pdf:
                    pdf = ''
            # print(f"...LESSON TRANSLATE CONFIRM - Language: {language}")
            
            # Ensure video is embed link
            video = request.form.get("inputLTVideo")
            if video and video != 'None':
                if 'https://www.youtube.com/embed/' not in video:
                    return apology("Youtube link must be from Embed link", 400)
                elif '"' in video:
                    return apology("Youtube link cannot include quotations", 400)
            
            # Update each field in lesson translations
            db.execute(
                """UPDATE lesson_translations SET (lt_language_id, lt_title, lt_subtitle, lt_overview, 
                lt_picture, lt_video, lt_text, lt_pdf_placeholder, lt_pdf, lt_link_placeholder, lt_link) = 
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) WHERE lt_lesson_id = ?""", 
                language, title, subtitle, overview, picture, video, text, pdf_placeholder, pdf, link_placeholder, link, id)

            # course_title = rows[0]['course_title']
            rows = db.execute(
                    """SELECT * FROM lessons WHERE lesson_section_id = ? ORDER BY lessons.lesson_number ASC""", section_id)
                # print(f"....LESSON: {rows}")

            return render_template("lessons.html", no_edit=flag, lessons=rows, section_id=section_id, course_title=course_title, 
                                   course_id=course_id, section_title=section_title)
        else:
            return apology("Course not found, Please define course id", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

# Fetch translation based on user's language
def get_lesson(lesson_id):
    user_lang = str(get_locale())

    if user_lang == 'fr':
        lang = 2
    else:
        lang = 1

    # Fetch the default section data
    lesson = db.execute("SELECT * FROM lessons WHERE lesson_id = ?", lesson_id)[0]

    # Fetch the translation based on the user's language
    translation = db.execute("SELECT * FROM lesson_translations WHERE lt_lesson_id = ? AND lt_language_id = ?", lesson_id, lang)
    if translation:
        translation = translation[0]
        return {
            'lesson_title': translation['lt_title'], # Translatable fields
            'lesson_subtitle': translation['lt_subtitle'],
            'lesson_overview': translation['lt_overview'],
            'lesson_picture': translation['lt_picture'],
            'lesson_video': translation['lt_video'],
            'lesson_link_placeholder': translation['lt_link_placeholder'],
            'lesson_link': translation['lt_link'],
            'lesson_text': translation['lt_text'],
            'lesson_pdf_placeholder': translation['lt_pdf_placeholder'],
            'lesson_pdf': translation['lt_pdf'],
            'lesson_number': lesson['lesson_number'], # Non-Translatable fields
            'lesson_published': lesson['lesson_published'],
            'lesson_id': lesson['lesson_id'],
            'lesson_section_id': lesson['lesson_section_id']
        }
    else:
        return {
            'lesson_title': lesson['lesson_title'],
            'lesson_subtitle': lesson['lesson_subtitle'],
            'lesson_overview': lesson['lesson_overview'],
            'lesson_picture': lesson['lesson_picture'],
            'lesson_video': lesson['lesson_video'],
            'lesson_link_placeholder': lesson['lesson_link_placeholder'],
            'lesson_link': lesson['lesson_link'],
            'lesson_text': lesson['lesson_text'],
            'lesson_pdf_placeholder': lesson['lesson_pdf_placeholder'],
            'lesson_pdf': lesson['lesson_pdf'],
            'lesson_number': lesson['lesson_number'],
            'lesson_published': lesson['lesson_published'],
            'lesson_id': lesson['lesson_id'],
            'lesson_section_id': lesson['lesson_section_id']
        }
    

@app.route('/lesson_detail/<int:lesson_id>', methods=["GET", "POST"])
@login_required
def lesson_detail(lesson_id):

    # Check if Admin User
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role =  user_rows[0]['role_id']

    # Flag to show admin nav
    if user_role == 1:
        flag = True
    
    else:
        flag = False

    # Set Default Flags: Section Flag to indicate if last lesson, quiz flag to indicate if quiz taken
    quiz_flag = False
    quiz_publish_flag = False
    sec_flag = False
    les_flag = False
    final_flag = False

    # Lessons info
    lesson = get_lesson(lesson_id)
    # print(f"....LESSON DETAIL - Lesson Info (lesson): {lesson}")
    # print(f"....LESSON DETAIL - Lesson ID (lesson_id): {lesson_id}")
    if not lesson:
        return apology("Lesson not found", 404)

    # Gather Course and Section info for page
    info = db.execute(
        """SELECT course_id, section_id, lesson_number FROM courses 
        JOIN sections ON courses.course_id = sections.section_course_id 
        JOIN lessons ON sections.section_id = lessons.lesson_section_id 
        WHERE lessons.lesson_id = ?""", lesson_id)
    # print(f"....LESSON DETAIL - Course and Section Info (info): {info}")
    if info:
        course_id = info[0]['course_id']
        section_id = info[0]['section_id']
        lesson_number = info[0]['lesson_number']

        # Get original and translation info
        course = get_course(course_id)
        section = get_section(section_id)

        course_title = course['course_title']
        section_number = section['section_number']
        section_title = section['section_title']

         # Default question flags
        question_1 = question_2 = question_3 = False
        # Get quiz id from lesson to gather quiz and questions
        q_info = db.execute("SELECT quiz_id FROM quiz WHERE quiz_lesson_id = ?", lesson_id)
        # print(f"....LESSON DETAIL - Quiz and Question info (q_info): {q_info}")
        if q_info:
            quiz_id = q_info[0]['quiz_id']
            # print(f"....LESSON DETAIL - Q_info (quiz_id): {quiz_id}")

            # Gather quiz and question info
            quiz = get_quiz(quiz_id)
            # print(f"....LESSON DETAIL - Get_Quiz (quiz): {quiz}")
            count = question_count(quiz_id)
            # print(f"....LESSON DETAIL - count: {count}")
            if count == 1:
                question_1 = get_question_1(quiz_id)
                # print(f"....LESSON DETAIL - question_1: {question_1}")
            elif count == 2:
                question_1 = get_question_1(quiz_id)
                question_2 = get_question_2(quiz_id)
                # print(f"....LESSON DETAIL - question_1: {question_1}")
                # print(f"....LESSON DETAIL - question_2: {question_2}")
            elif count == 3:
                question_1 = get_question_1(quiz_id)
                question_2 = get_question_2(quiz_id)
                question_3 = get_question_3(quiz_id)
                # print(f"....LESSON DETAIL - question_1: {question_1}")
                # print(f"....LESSON DETAIL - question_2: {question_2}")
                # print(f"....LESSON DETAIL - question_3: {question_3}")
            else:
                flag ("Lesson_detail error, no quiz found")
        
            # quizzes = [quiz, question_1]
            # print(f"....LESSON DETAIL - Translated Quiz and Question info (quizzes): {quizzes}")

            # Select Quiz Header, subtitle and id
            if quiz:
                quiz_title = quiz['quiz_title']
                quiz_subtitle = quiz['quiz_subtitle']
                quiz_id = quiz['quiz_id']
                quiz_published = quiz['quiz_published']

                # print(f"....LESSON DETAIL - quiz_title: {quiz_title}")
                # print(f"....LESSON DETAIL - quiz_subtitle: {quiz_subtitle}")
                # print(f"....LESSON DETAIL - quiz_id: {quiz_id}")
                # print(f"....LESSON DETAIL - quiz_published: {quiz_published}")

                if quiz_published == 1:
                    quiz_publish_flag = True
                # print(f"....LESSON DETAIL - Quiz ID: {quiz_id}")

        # LESSON GRADES/QUIZ RESULTS
        # Check if User is already enrolled in lesson grades
        lesson_grades = db.execute("SELECT lesson_started FROM lesson_grades WHERE lg_lesson_id = ? AND lg_user_id = ?", lesson_id, users_id)
    
        # If not enrolled, enroll user
        if not lesson_grades:
            db.execute(
                """INSERT INTO lesson_grades (lesson_started, lg_lesson_id, lg_user_id) VALUES (?, ?, ?)""", 1, lesson_id, users_id)
            
        # Check if User took lesson quiz, show results
        if q_info:
            quiz_info = db.execute("SELECT quiz_grade FROM quiz_grades WHERE qg_quiz_id = ? AND qg_user_id = ?", quiz_id, users_id)
            if quiz_info:
                quiz_grade = quiz_info[0]['quiz_grade']
                quiz_flag = True
        

        # Select all lessons in the section (important to determine prev, current, next actions)
        sections_info = db.execute(
            """SELECT lesson_number, lesson_id FROM lessons WHERE lesson_section_id = ? ORDER BY lesson_number ASC""", section_id)
        # print(f"....LESSON DETAIL - Lesson in Section (sections_info): {sections_info}")

        if sections_info:

            # Get time for database updates:
            now_utc = datetime.now(pytz.UTC)

            # Previous Button Check:
            # Default Flags:
            prev_section = False
            prev_lesson = False
            
            # Is this first or last lesson in the section?
            first_lesson_id = sections_info[0]['lesson_id']
            first_lesson_number = sections_info[0]['lesson_number']
            last_lesson_id = sections_info[-1]['lesson_id']
            last_lesson_number = sections_info[-1]['lesson_number']
            # print(f"....LESSON DETAIL - First lesson ID: {first_lesson_id}")
            # print(f"....LESSON DETAIL - Last lesson ID: {last_lesson_id}")
            
            # Determine if we are in first lesson, get section id
            if first_lesson_id == lesson_id:
                prev_section = section_id

            # Loop through lessons to determine current, prev, and next info
            for index, lessons in enumerate(sections_info):
                current_lesson_id = sections_info[index]['lesson_id']
                current_lesson_number = sections_info[index]['lesson_number']
                
                # TODO: OPTION 1: IF MORE LESSONS IN SECTION
                if current_lesson_id == lesson_id and current_lesson_id != last_lesson_id:

                    # print(f"....LESSON DETAIL - Current Lesson (current_lesson_id): {current_lesson_id}")

                    # next_lesson_in_section Flag 
                    les_flag = True
                    sec_flag = False
                    final_flag = False

                    next_lesson_id = sections_info[index + 1]['lesson_id']
                    next_lesson_number = sections_info[index + 1]['lesson_number']
                    # print(f"....LESSON DETAIL - Next Lesson: {next_lesson_id}")

                    # Next lesson ID
                    next_id = next_lesson_id

                    # Ensure not first lesson in section for Prev Lesson button
                    if current_lesson_id != first_lesson_id:
                        prev_lesson = sections_info[index - 1]['lesson_id']
                        # print(f"....LESSON DETAIL - Prev Lesson: {prev_lesson}")

                    # IF not first Lesson, Update user's PREVIOUS lesson grades (completed, completed time, lesson grade) AND Section's grade
                    if current_lesson_id != first_lesson_id:
                        update_lg_sg(section_id, prev_lesson, users_id)
                else:
                    # TODO: Option 2: IF LAST LESSON IN SECTION, GO TO NEXT SECTION    
                    if current_lesson_id == lesson_id and current_lesson_id == last_lesson_id:

                        # print(f"....LESSON DETAIL - Current Lesson (current_lesson_id): {current_lesson_id}")

                        # Check all Sections in course
                        sections_row = db.execute(
                            """SELECT section_number, section_id FROM sections 
                            JOIN courses ON sections.section_course_id = courses.course_id WHERE course_id = ? ORDER BY section_number ASC""", course_id)
                        # print(f"....LESSON DETAIL - All Course Sections: {sections_row}")

                        if not sections_row:
                            return apology("No sections in course", 404)

                        # Get last section number in course
                        last_section_id = sections_row[-1]['section_id']
                        last_section = sections_row[-1]['section_number']
                        last_section_number = int(last_section)
                        # print(f"....LESSON DETAIL - LAST Section in course: {last_section_number}")

                        # Calculate next Section number
                        next_section_number = int(section_number) + 1
                        # print(f"....LESSON DETAIL - NEXT Section in course: {next_section_number}")
                        
                        # Prev btn for last lesson in section 
                        prev_lesson = sections_info[index - 1]['lesson_id']
                        # print(f"....LESSON DETAIL - Prev Lesson: {prev_lesson}")

                        # TODO: Option 2.A: IF THERE ARE MORE SECTIONS IN COURSE 
                        if next_section_number <= last_section_number:
                            next_section = db.execute("SELECT section_id FROM sections WHERE section_number = ? AND section_course_id = ?", 
                                                        next_section_number, course_id)

                            if next_section:
                                next_id = next_section[0]['section_id']
                                # print(f"....LESSON DETAIL - NEXT Section Number in course: {next_section_number}")

                                # If sections left in course, signal to go to next section
                                les_flag = False
                                sec_flag = True
                                final_flag = False
                            else:
                                return apology("No more section error", 400)
                            
                            # If only one lesson in section, do not update lesson grades
                            # IF THERE WAS A PREV LESSON IN THIS SECTION: Update user's PREVIOUS lesson grades (completed, completed time, lesson grade) AND Section's grade
                            update_lg_sg(section_id, prev_lesson, users_id)

                        # TODO: Option 2.B: IF THIS IS THE LAST LESSON IN LAST SECTION OF COURSE (FINAL LESSON - CERTIFICATE) 
                        else:

                            # final_flag = True
                            les_flag = False
                            sec_flag = False
                            final_flag = True

                            # TODO: Where to go if course complete? (DASHBOARD)
                            next_id = None 

                            # Ensure it was the only lesson in the section
                            if current_lesson_id != first_lesson_id:
                                # Update user's lesson grades - mark prev lesson (completed, completed time, grade) AND Section's Grade
                                update_lg_sg(section_id, prev_lesson, users_id)
                                # Update user's section grades - mark current section (completed, completed time)
                                mark_sec_complete(section_id, users_id)
                                
                                # Update user's course grades - mark course (completed, completed time, grade)
                                update_course_final_grade(course_id, users_id)
                                mark_course_complete(course_id, users_id)
                            else:
                                # Update user's section grades - mark current section (completed, completed time, grade)
                                # Update user's course grades - mark course (completed, completed time, grade)
                                update_final_lesson(lesson_id, section_id, course_id, users_id)

                # Next_id will equal either section or lesson, the flag will determine which route to enable on front end)
                
                # Return if there is a quiz
            if q_info:
                if quiz_info:
                    # Return if already took the quiz
                    return render_template('lesson_detail.html', lesson=lesson, users=flag, section_flag=sec_flag, lesson_flag=les_flag,   # Main info
                                           course_title=course_title, section_title=section_title, section_number=section_number,          # Course Info
                                           quiz_taken=quiz_flag, quiz_published=quiz_publish_flag, quiz=quiz, quiz_title=quiz_title,        # Quiz info
                                           quiz_subtitle=quiz_subtitle, quiz_id=quiz_id, quiz_grade=quiz_grade,
                                           question_1=question_1, question_2=question_2, question_3=question_3,                             # Question info
                                           prev_section=prev_section, prev_lesson=prev_lesson, next_id=next_id)                            # Button info
                else:
                    # Return if quiz not taken yet 
                    return render_template('lesson_detail.html', lesson=lesson, users=flag, section_flag=sec_flag, lesson_flag=les_flag,   # Main info
                                           course_title=course_title, section_title=section_title, section_number=section_number,          # Course Info
                                           quiz_published=quiz_publish_flag, quiz_taken=quiz_flag, quiz=quiz, quiz_title=quiz_title,        # Quiz info
                                           quiz_subtitle=quiz_subtitle, quiz_id=quiz_id,
                                           question_1=question_1, question_2=question_2, question_3=question_3,                             # Question info
                                           prev_section=prev_section, prev_lesson=prev_lesson, next_id=next_id)                            # Button info

            else:
                # RETURN IF MORE LESSONS IN CURRENT SECTION OR OTHER SECTIONS
                return render_template('lesson_detail.html', lesson=lesson, users=flag, section_flag=sec_flag, lesson_flag=les_flag,  # Main info
                                       course_title=course_title, section_title=section_title, section_number=section_number,         # Course info
                                       prev_section=prev_section, prev_lesson=prev_lesson, next_id=next_id)                           # Button info
                
    else:   
        # Return apology if no info
        return apology("Can't find lesson", 404)
            

# """ ----------------------- LESSON END ----------------------- ""

# """ ----------------------- QUIZ START ----------------------- ""

@app.route("/quiz", methods=["GET", "POST"])
@login_required
def quiz():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:
    
        # Flag for edit button template
        flag = True
        # print(f"....QUIZ - flag: {flag}")

        # Get lesson id
        lesson_id = request.form.get("quiz_lesson_id")

        # Flag for quiz add button
        quiz_exist = db.execute("SELECT quiz_id, quiz_lesson_id FROM quiz WHERE quiz_lesson_id = ?", lesson_id)

        if quiz_exist:
            add_flag = False
        else:
            add_flag = True

        # Display the quizzes in the database 
        rows = db.execute(
             """SELECT quiz_id, course_title, section_number, lesson_number, lesson_title, quiz_number, quiz_published 
             FROM quiz JOIN lessons ON quiz.quiz_lesson_id = lessons.lesson_id
             JOIN sections ON lessons.lesson_section_id = sections.section_id
             JOIN courses ON sections.section_course_id = courses.course_id 
             WHERE quiz_lesson_id = ? ORDER BY course_title ASC, section_number ASC, lesson_number ASC, quiz_number ASC""", lesson_id)

        return render_template("quiz.html", quizzes=rows, no_edit=flag, lesson_id=lesson_id, yes_add_quiz=add_flag)
    
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/add_quiz", methods=["POST"])
@login_required
def add_quiz():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:
        
        # Flag for add button template
        flag = True
        
        lesson_id = request.form.get("lesson_id")

        return render_template("quiz.html", yes_add=flag, lesson_id=lesson_id)
    
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/quiz_add_confirm", methods=["POST"])
@login_required
def quiz_add_confirm():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for edit button template
        flag = True

        # Lesson 
        lesson_id = request.form.get("quiz_lesson_id")

        # Quiz Title
        title = request.form.get("inputTitle")
        if not title:
            return apology("Please enter Title", 400)
        
        # Quiz Number (AUTOMATIC BASED ON COUNT OF QUIZZES IN LESSON, NOT USER INPUT)
        quiz_list = db.execute(
            """SELECT COUNT(quiz_number) FROM quiz WHERE quiz_lesson_id = ?""", lesson_id)
        
        # print(f"....QUIZ ADD CONFIRM - Quiz_List: {quiz_list}")
        
        if quiz_list:
            quiz_count = quiz_list[0]['COUNT(quiz_number)']
            quiz_number = quiz_count + 1

            # print(f"....QUIZ ADD CONFIRM - Quiz_Number: {quiz_number}")
        
        # Subtitle
        subtitle = request.form.get("inputSubtitle")

        # Verify and upload Published
        published = request.form.get("inputPublished")
        if published:
            published = int(published)

        # INSERT INFO INTO QUIZ DATABASE
        db.execute("""
                    INSERT INTO quiz (quiz_title, quiz_subtitle, quiz_lesson_id, quiz_number, quiz_published) 
                   VALUES (?, ?, ?, ?, ?)""", title, subtitle, lesson_id, quiz_number, published)
        
        # GET QUIZ ID
        quiz = db.execute(
            """SELECT quiz_id FROM quiz WHERE quiz_lesson_id = ? AND quiz_number = ?""", lesson_id, quiz_number)
        
        quiz_id = quiz[0]['quiz_id']
        # print(f" QUIZ ADD CONFIRM - QUIZ ID: {quiz_id}")
        
        # QUESTIONS DATABASE

        # Question 1: 
        question1 = request.form.get("inputQuestion1")
        if not question1:
            return apology("Please enter question 1", 400)
        
        if question1:
            question_number = 1
        
        question1_answer1 = request.form.get("inputAnswer1_Q1")
        if not  question1_answer1:
            return apology("Please enter answer 1 for question 1", 400)

        question1_answer2 = request.form.get("inputAnswer2_Q1")
        if not  question1_answer2:
            return apology("Please enter answer 2 for question 2", 400)
        
        question1_answer3 = request.form.get("inputAnswer3_Q1")

        question1_answer4 = request.form.get("inputAnswer4_Q1")
        
        question1_correct = request.form.get("question1_correct")
        if not question1_correct or question1_correct == "0":
            return apology("Please enter correct answer for question 1", 400)
        
        # INSERT QUESTION 1 INFO INTO QUESTIONS DATABASE
        db.execute(
            """INSERT INTO questions (question_number, question, answer_1, answer_2, answer_3, answer_4, 
            correct_answer, question_quiz_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", question_number, question1, 
            question1_answer1, question1_answer2, question1_answer3, question1_answer4, question1_correct, quiz_id)
        
        # Question 2: 
        question2 = request.form.get("inputQuestion2")
        
        if question2:
            question_number = 2
        
        question2_answer1 = request.form.get("inputAnswer1_Q2")
        if question2:
            if not question2_answer1:
                return apology("Please enter answer 1 for question 2", 400)

        question2_answer2 = request.form.get("inputAnswer2_Q2")
        if question2:
            if not question2_answer2:
                return apology("Please enter answer 2 for question 2", 400)
        
        question2_answer3 = request.form.get("inputAnswer3_Q2")

        question2_answer4 = request.form.get("inputAnswer4_Q2")
        
        question2_correct = request.form.get("question2_correct")
        if question2:
            if not question2_correct or question2_correct == "0":
                return apology("Please enter correct answer for question 2", 400)
        
        # INSERT QUESTION 2 INFO INTO QUESTIONS DATABASE
        if question2:
            db.execute(
                """INSERT INTO questions (question_number, question, answer_1, answer_2, answer_3, 
                answer_4, correct_answer, question_quiz_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", question_number, question2, 
                question2_answer1, question2_answer2, question2_answer3, question2_answer4, question2_correct, quiz_id)

        # Question 3: 
        question3 = request.form.get("inputQuestion3")
        
        if question3:
            question_number = 3
        
        question3_answer1 = request.form.get("inputAnswer1_Q3")
        if question3:
            if not question3_answer1:
                return apology("Please enter answer 1 for question 3", 400)

        question3_answer2 = request.form.get("inputAnswer2_Q3")
        if question3:
            if not question3_answer2:
                return apology("Please enter answer 2 for question 3", 400)
        
        question3_answer3 = request.form.get("inputAnswer3_Q3")

        question3_answer4 = request.form.get("inputAnswer4_Q3")
        
        question3_correct = request.form.get("question3_correct")
        if question3:
            if not question3_correct or question3_correct == "0":
                return apology("Please enter correct answer for question 3", 400)
        
        # INSERT QUESTION 3 INFO INTO QUESTIONS DATABASE
        if question3:
            db.execute(
                """INSERT INTO questions (question_number, question, answer_1, answer_2, answer_3, answer_4, correct_answer, question_quiz_id) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", question_number, question3, question3_answer1, question3_answer2, question3_answer3, 
                question3_answer4, question3_correct, quiz_id)
        
        # Display the quizzes in the database 
        rows = db.execute(
             """SELECT quiz_id, course_title, section_number, lesson_number, lesson_title, quiz_number, quiz_published 
             FROM quiz JOIN lessons ON quiz.quiz_lesson_id = lessons.lesson_id 
             JOIN sections ON lessons.lesson_section_id = sections.section_id 
             JOIN courses ON sections.section_course_id = courses.course_id 
             WHERE quiz_lesson_id = ? ORDER BY course_title ASC, section_number ASC, lesson_number ASC, quiz_number ASC""", lesson_id)
        
        # print(f"....QUIZ ADD CONFIRM - rows: {rows}")

        # return redirect("/quiz")
        return render_template("quiz.html", quizzes=rows, no_edit=flag, lesson_id=lesson_id)
    
    else:
        return apology("You do not have permission to add a quiz", 400)
    

@app.route("/quiz_edit", methods=["POST"])
@login_required
def quiz_edit():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for quiz edit button
        flag = True

        # Query the quiz in the database 
        # rows = db.execute("SELECT * FROM quiz")

        # Select quiz to edit
        id = request.form.get("quiz_edit_id")
        if id:

            quiz_lesson_id = request.form.get("quiz_lesson_id")

            # Select all quiz information to show
            rows = db.execute(
                """SELECT course_title, section_number, lesson_number, lesson_title, lesson_id, quiz_title, 
                quiz_subtitle, quiz_number, quiz_published, quiz_id, quiz_published
                FROM courses JOIN sections ON courses.course_id = sections.section_course_id
                 JOIN lessons ON sections.section_id = lessons.lesson_section_id 
                 JOIN quiz ON quiz.quiz_lesson_id = lessons.lesson_id WHERE quiz_id = ?""", id)
            if rows:
                row = rows
            # print(f"...QUIZ EDIT - Quizzes:{row}")

            # Select all question information to show
            questions = db.execute(
                """SELECT * FROM questions WHERE question_quiz_id = ? ORDER BY question_number ASC""", id)
            if questions:
                questions = questions
            # print(f"...QUIZ EDIT - Questions:{questions}")
            
            # Select all lesson info to show
            lesson_info = db.execute(
            """SELECT lesson_id, lesson_title, lesson_number, section_number, course_title FROM lessons 
            JOIN sections ON lessons.lesson_section_id = sections.section_id 
            JOIN courses ON sections.section_course_id = courses.course_id 
            ORDER BY course_title ASC, section_number ASC, lesson_number ASC""")
            if lesson_info:
                lessons = lesson_info

            # print(f"...QUIZ EDIT - LESSONS:{lessons}")
                
            return render_template("quiz.html", yes_edit=flag, quizzes=row, questions=questions, lessons=lessons, edit_quiz_id=id, 
                                   quiz_lesson_id=quiz_lesson_id)
        
        else:
            return apology("Course not found, Please define course id", 400)
    else:
        return apology("You do not have permission to access this page", 400)


@app.route("/quiz_edit_confirm", methods=["POST"])
@login_required
def quiz_edit_confirm():

     # Ensure valid admin user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Select quiz to edit
        id = request.form.get("quiz_edit_id")

        if id:
            
            # QUIZ DATABASE
            # Flag for edit button template
            flag = True
            
            # GET QUIZ ID
            quiz_id = request.form.get("quiz_edit_id")

            # print(f"....QUIZ edit CONFIRM TOP - Quiz_id: {quiz_id}")

            # Lesson 
            lesson_id = request.form.get("quiz_lesson_id")

            # print(f"....QUIZ edit CONFIRM TOP- lesson_id: {lesson_id}")

            # Quiz Title
            title = request.form.get("inputTitle")
            if not title:
                return apology("Please enter Title", 400)
            
            # Subtitle
            subtitle = request.form.get("inputSubtitle")

            # Verify and upload Published
            published = request.form.get("inputPublished")
            if published:
                published = int(published)

            # UPDATE INFO IN QUIZ DATABASE
            db.execute("""
                        UPDATE quiz SET (quiz_title, quiz_subtitle, quiz_published) 
                    = (?, ?, ?) WHERE quiz_id = ?""", title, subtitle, published, quiz_id)
            
            # QUESTIONS DATABASE

            # Question 1: 
            question1 = request.form.get("inputQuestion1")
            if not question1:
                return apology("Please enter question 1", 400)
            
            if question1:
                question1_number = 1
            
            question1_answer1 = request.form.get("inputAnswer1_Q1")
            if not  question1_answer1:
                return apology("Please enter answer 1 for question 1", 400)

            question1_answer2 = request.form.get("inputAnswer2_Q1")
            if not  question1_answer2:
                return apology("Please enter answer 2 for question 2", 400)
            
            question1_answer3 = request.form.get("inputAnswer3_Q1")

            question1_answer4 = request.form.get("inputAnswer4_Q1")
            
            question1_correct = request.form.get("question1_correct")
            if not  question1_correct:
                return apology("Please enter correct answer for question 1", 400)
            
            # INSERT QUESTION 1 INFO INTO QUESTIONS DATABASE
            db.execute(
                """UPDATE questions SET (question, answer_1, answer_2, answer_3, answer_4, correct_answer) 
                = (?, ?, ?, ?, ?, ?) WHERE question_quiz_id = ? AND question_number = ?""", question1, question1_answer1, 
                question1_answer2, question1_answer3, question1_answer4, question1_correct, quiz_id, question1_number)
            
            # Question 2: 
            question2 = request.form.get("inputQuestion2")
            
            if question2:
                question2_number = 2
            
            question2_answer1 = request.form.get("inputAnswer1_Q2")
            if question2:
                if not question2_answer1:
                    return apology("Please enter answer 1 for question 2", 400)

            question2_answer2 = request.form.get("inputAnswer2_Q2")
            if question2:
                if not question2_answer2:
                    return apology("Please enter answer 2 for question 2", 400)
            
            question2_answer3 = request.form.get("inputAnswer3_Q2")

            question2_answer4 = request.form.get("inputAnswer4_Q2")
            
            question2_correct = request.form.get("question2_correct")
            if question2:
                if not question2_correct:
                    return apology("Please enter correct answer for question 2", 400)
            
            # INSERT QUESTION 2 INFO INTO QUESTIONS DATABASE
            if question2:
                db.execute(
                    """UPDATE questions SET (question, answer_1, answer_2, answer_3, answer_4, correct_answer) 
                    = (?, ?, ?, ?, ?, ?) WHERE question_quiz_id = ? AND question_number = ?""", question2, question2_answer1, 
                    question2_answer2, question2_answer3, question2_answer4, question2_correct, quiz_id, question2_number)

            # Question 3: 
            question3 = request.form.get("inputQuestion3")
            
            if question3:
                question3_number = 3
            
            question3_answer1 = request.form.get("inputAnswer1_Q3")
            if question3:
                if not question3_answer1:
                    return apology("Please enter answer 1 for question 3", 400)

            question3_answer2 = request.form.get("inputAnswer2_Q3")
            if question3:
                if not question3_answer2:
                    return apology("Please enter answer 2 for question 3", 400)
            
            question3_answer3 = request.form.get("inputAnswer3_Q3")

            question3_answer4 = request.form.get("inputAnswer4_Q3")
            
            question3_correct = request.form.get("question3_correct")
            if question3:
                if not question3_correct:
                    return apology("Please enter correct answer for question 3", 400)
            
            # INSERT QUESTION 3 INFO INTO QUESTIONS DATABASE
            if question3:
                db.execute(
                    """UPDATE questions SET (question, answer_1, answer_2, answer_3, answer_4, correct_answer) 
                    = (?, ?, ?, ?, ?, ?) 
                    WHERE question_quiz_id = ? AND question_number = ?""", question3, question3_answer1, question3_answer2, question3_answer3, 
                    question3_answer4, question3_correct, quiz_id, question3_number)
                

            # UPDATE QUIZ NUMBERS (AUTOMATIC BASED ON COUNT OF QUIZZES IN LESSON)
            quiz_list = db.execute("SELECT quiz_id, quiz_lesson_id, quiz_number FROM quiz WHERE quiz_lesson_id = ?", lesson_id)

            # print(f"....QUIZ EDIT CONFIRM - Quiz_List: {quiz_list}")

            if quiz_list:
                # sort quiz_list by 'quiz_number'
                quiz_list.sort(key=lambda x: x['quiz_number'])

                # iterate over sorted list
                for i, quiz in enumerate(quiz_list):
                    # update 'quiz_number' to match position in list
                    quiz['quiz_number'] = i + 1
                    db.execute("UPDATE quiz SET (quiz_number) = (?) WHERE quiz_id = ?", i + 1, quiz['quiz_id'])

                # print(f"....QUIZ EDIT CONFIRM - UPdated Quiz_List: {quiz_list}")                   

            # Display the quizzes in the database 
            rows = db.execute(
             """SELECT quiz_id, course_title, section_number, lesson_number, lesson_title, quiz_number, quiz_published 
             FROM quiz JOIN lessons ON quiz.quiz_lesson_id = lessons.lesson_id 
             JOIN sections ON lessons.lesson_section_id = sections.section_id 
             JOIN courses ON sections.section_course_id = courses.course_id 
             WHERE quiz_lesson_id = ? ORDER BY course_title ASC, section_number ASC, lesson_number ASC, quiz_number ASC""", lesson_id)
        
            # print(f"....QUIZ edit CONFIRM - rows: {rows}")
            # print(f"....QUIZ edit CONFIRM - lesson_id: {lesson_id}")

            # return redirect("/quiz")
            return render_template("quiz.html", quizzes=rows, no_edit=flag, lesson_id=lesson_id)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/delete_quiz", methods=["POST"])
@login_required
def delete_quiz():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Delete Quiz
        id = request.form.get("delete_quiz_id")
     
        # print(f"..DELETE Quiz ID: {id}")
        if id:
            
            db.execute("DELETE FROM quiz WHERE quiz_id = ?", id)

            # Flag for edit button template
            flag = True

            # Lesson to return to
            lesson_id = request.form.get("lesson_edit_id")
           
            # print(f"....DELETE QUIZ - Lesson ID: {lesson_id}")

            # UPDATE QUIZ NUMBERS (AUTOMATIC BASED ON COUNT OF QUIZZES IN LESSON)
            quiz_list = db.execute("SELECT quiz_id, quiz_lesson_id, quiz_number FROM quiz WHERE quiz_lesson_id = ?", lesson_id)

            # print(f"....QUIZ EDIT CONFIRM - Quiz_List: {quiz_list}")

            if quiz_list:
                # sort quiz_list by 'quiz_number'
                quiz_list.sort(key=lambda x: x['quiz_number'])

                # iterate over sorted list
                for i, quiz in enumerate(quiz_list):
                    # update 'quiz_number' to match position in list
                    quiz['quiz_number'] = i + 1
                    db.execute("UPDATE quiz SET (quiz_number) = (?) WHERE quiz_id = ?", i + 1, quiz['quiz_id'])

                # print(f"....QUIZ EDIT CONFIRM - UPdated Quiz_List: {quiz_list}")

            # Display the quizzes in the database 
            rows = db.execute(
             """SELECT quiz_id, course_title, section_number, lesson_number, lesson_title, quiz_number, quiz_published 
             FROM quiz JOIN lessons ON quiz.quiz_lesson_id = lessons.lesson_id 
             JOIN sections ON lessons.lesson_section_id = sections.section_id 
             JOIN courses ON sections.section_course_id = courses.course_id 
             WHERE quiz_lesson_id = ? ORDER BY course_title ASC, section_number ASC, lesson_number ASC, quiz_number ASC""", lesson_id)
            
            # Flag for quiz add button
            quiz_exist = db.execute("SELECT quiz_id, quiz_lesson_id FROM quiz WHERE quiz_lesson_id = ?", lesson_id)

            if quiz_exist:
                add_flag = False
            else:
                add_flag = True
        
            # print(f"....QUIZ DELETE CONFIRM - rows: {rows}")
            # print(f"....QUIZ DELETE CONFIRM - lesson_id: {lesson_id}")

            # return redirect("/quiz")
            return render_template("quiz.html", quizzes=rows, no_edit=flag, lesson_id=lesson_id, yes_add_quiz=add_flag)
    
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/quiz_translate", methods=["POST"])
@login_required
def quiz_translate():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for quiz edit translation button
        flag = True

        # Select quiz to edit
        id = request.form.get("quiz_edit_id")
        if id:
            # print(f"...QUIZ TRANSLATE  - Quiz Id:{id}")
            # Get the quiz lesson id
            quiz_lesson_id = request.form.get("quiz_lesson_id")

            # Display the QUIZ info from the database 
            quiz_rows = db.execute(
                """SELECT quiz_id, quiz_title, quiz_subtitle FROM quiz WHERE quiz_id = ?""", id)
            # print(f"...QUIZ TRANSLATE - ROWS:{quiz_rows}")

            # Display the QUESTIONS information to show
            questions = db.execute(
                """SELECT * FROM questions WHERE question_quiz_id = ? ORDER BY question_number ASC""", id)
            if questions:
                # print(f"...QUIZ TRANSLATE - Questions:{questions}")

                # Determine question count to get question id:
                question_count = len(questions)
                # print(f"...QUIZ TRANSLATE - Question Count:{question_count}")
                if question_count == 1:
                    question_1_id = questions[0]['question_id']
                    question_1_number = 1
                    # print(f"...QUIZ TRANSLATE - Question 1 ID:{question_1_id}")

                elif question_count == 2:
                    question_1_id = questions[0]['question_id']
                    question_1_number = 1
                    question_2_id = questions[1]['question_id']
                    question_2_number = 2

                elif question_count == 3:
                    question_1_id = questions[0]['question_id']
                    question_1_number = 1
                    question_2_id = questions[1]['question_id']
                    question_2_number = 2
                    question_3_id = questions[2]['question_id']
                    question_3_number = 3
                else:
                    return apology("Either no quiz or Add question count in Quiz translate", 400)
            else:
                return apology("Add questions to this quiz", 400)
            
            # Default answer count flags
            q1_answer_3 = q1_answer_4 = q2_answer_3 = q2_answer_4 = q3_answer_3 = q3_answer_4 = False
            
            # TODO: config a better system
            # Check answer count to flag translation
            if question_count == 1:
                if questions[0]['answer_3'] != '':
                    q1_answer_3 = True
                if questions[0]['answer_4'] != '':
                    q1_answer_4 = True
            elif question_count == 2:
                if questions[0]['answer_3'] != '':
                    q1_answer_3 = True
                if questions[0]['answer_4'] != '':
                    q1_answer_4 = True
                if questions[1]['answer_3'] != '':
                    q2_answer_3 = True
                if questions[1]['answer_4'] != '':
                    q2_answer_4 = True
            elif question_count == 3:
                if questions[0]['answer_3'] != '':
                    q1_answer_3 = True
                if questions[0]['answer_4'] != '':
                    q1_answer_4 = True
                if questions[1]['answer_3'] != '':
                    q2_answer_3 = True
                if questions[1]['answer_4'] != '':
                    q2_answer_4 = True
                if questions[2]['answer_3'] != '':
                    q3_answer_3 = True
                if questions[2]['answer_4'] != '':
                    q3_answer_4 = True
            else:
                return apology("Either no quiz or Add question count in Quiz translate", 400)
            
            # Select all fields to populate in form-select types
            languages = db.execute("SELECT language_id, language FROM languages")
            # print(f"...QUIZ TRANSLATE  - Languages:{language}")

            # Check if current QUIZ translations to show
            qz_translations = db.execute(
                """SELECT * FROM quiz_translations WHERE qzt_quiz_id = ?""", id)
            if qz_translations:
                # print(f"...QUIZ TRANSLATE  - QUIZ Translations:{qz_translations}")
                # Get translation id
                qzt_id = qz_translations[0]['qzt_id']
            else:
                # Insert into translations and get info
                db.execute("INSERT INTO quiz_translations (qzt_quiz_id) VALUES (?)", id)
                qz_translations = db.execute("SELECT * FROM quiz_translations WHERE qzt_quiz_id = ?", id)
                qzt_id = qz_translations[0]['qzt_id']

            # Check if current QUESTIONS translations to show
            qn_translations = db.execute(
                """SELECT * FROM question_translations WHERE qnt_quiz_id = ? ORDER BY qnt_question_number ASC""", id)
            if qn_translations:
                # print(f"...QUIZ TRANSLATE  - QUESTION Translations (if qn_tran..):{qn_translations}")
                qn_translations = qn_translations
            else:
                 # Insert into translations and get info
                # Check each question:
                if question_count == 1:
                    db.execute(
                        """INSERT INTO question_translations (qnt_quiz_id, qnt_question_id, qnt_question_number) VALUES (?, ?, ?)""", 
                        id, question_1_id, question_1_number)
                elif question_count == 2:
                    db.execute("""INSERT INTO question_translations (qnt_quiz_id, qnt_question_id, qnt_question_number) VALUES (?, ?, ?)""", 
                               id, question_1_id, question_1_number)
                    db.execute("""INSERT INTO question_translations (qnt_quiz_id, qnt_question_id, qnt_question_number) VALUES (?, ?, ?)""", 
                               id, question_2_id, question_2_number)
                elif question_count == 3:
                    db.execute("""INSERT INTO question_translations (qnt_quiz_id, qnt_question_id, qnt_question_number) VALUES (?, ?, ?)""", 
                               id, question_1_id, question_1_number)
                    db.execute("""INSERT INTO question_translations (qnt_quiz_id, qnt_question_id, qnt_question_number) VALUES (?, ?, ?)""", 
                               id, question_2_id, question_2_number)
                    db.execute("""INSERT INTO question_translations (qnt_quiz_id, qnt_question_id, qnt_question_number) VALUES (?, ?, ?)""", 
                               id, question_3_id, question_3_number)
                else:
                    return apology("Either no quiz or Add question count in Quiz translate", 400)

                qn_translations = db.execute("SELECT * FROM question_translations WHERE qnt_quiz_id = ?", id)
                # print(f"...QUIZ TRANSLATE  - QUESTION Translations (else qn_tran..):{qn_translations}")
            # Display the quizzes of the lesson for exit 
            lesson_quizzes = db.execute(
                """SELECT quiz_id, course_title, section_number, lesson_number, lesson_title, quiz_number, quiz_published 
                FROM quiz JOIN lessons ON quiz.quiz_lesson_id = lessons.lesson_id
                JOIN sections ON lessons.lesson_section_id = sections.section_id
                JOIN courses ON sections.section_course_id = courses.course_id 
                WHERE quiz_lesson_id = ? ORDER BY course_title ASC, section_number ASC, lesson_number ASC, quiz_number ASC""", quiz_lesson_id)

            return render_template("quiz.html", yes_edit_translate=flag, quizzes=lesson_quizzes, quiz_rows=quiz_rows, questions=questions, 
                                   quiz_translations=qz_translations, question_translations=qn_translations, languages=languages, edit_quiz_id=id, 
                                   qzt_id=qzt_id, quiz_lesson_id=quiz_lesson_id,
                                   q1_answer_3=q1_answer_3, q1_answer_4=q1_answer_4, q2_answer_3=q2_answer_3, q2_answer_4=q2_answer_4,
                                   q3_answer_3=q3_answer_3, q3_answer_4=q3_answer_4)
        
        else:
            return apology("Quiz not found, Please define quiz id", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/quiz_translate_confirm", methods=["POST"])
@login_required
def quiz_translate_confirm():

     # Ensure valid admin user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # QUIZ TRANSLATION DATABASE
        # Flag for edit button template
        flag = True
        
        # Get general info:
        qzt_id = request.form.get("quiz_translate_id")
        quiz_id = request.form.get("quiz_edit_id")
        quiz_lesson_id = request.form.get("quiz_lesson_id")

        # Quiz Translate Info
        title = request.form.get("inputQZTTitle")
        subtitle = request.form.get("inputQZTSubtitle")
        language = request.form.get("inputQZTLanguage")

        # UPDATE INFO IN QZT DATABASE
        db.execute("""
                    UPDATE quiz_translations SET (qzt_title, qzt_subtitle, qzt_language_id) 
                = (?, ?, ?) WHERE qzt_id = ?""", title, subtitle, language, qzt_id)
        
        # QUESTIONS TRANSLATION DATABASE
        # Question 1: 
        question1 = request.form.get("inputQNTQuestion1")
        if not question1:
            return apology("Please enter question 1", 400)
        if question1:
            question1_number = 1
        
        question1_answer1 = request.form.get("inputQNTAnswer1_Q1")
        question1_answer2 = request.form.get("inputQNTAnswer2_Q1")
        question1_answer3 = request.form.get("inputQNTAnswer3_Q1")
        question1_answer4 = request.form.get("inputQNTAnswer4_Q1")
        
        # INSERT QUESTION 1 INFO INTO QUESTIONS TRANSLATION DATABASE
        db.execute(
            """UPDATE question_translations SET (qnt_question, qnt_answer_1, qnt_answer_2, qnt_answer_3, qnt_answer_4, qnt_language_id) 
            = (?, ?, ?, ?, ?, ?) WHERE qnt_quiz_id = ? AND qnt_question_number = ?""", question1, question1_answer1, 
            question1_answer2, question1_answer3, question1_answer4, language, quiz_id, question1_number)
        
        # Question 2: 
        question2 = request.form.get("inputQNTQuestion2")
        if question2:
            question2_number = 2
        
        question2_answer1 = request.form.get("inputQNTAnswer1_Q2")
        question2_answer2 = request.form.get("inputQNTAnswer2_Q2")
        question2_answer3 = request.form.get("inputQNTAnswer3_Q2")
        question2_answer4 = request.form.get("inputQNTAnswer4_Q2")

        # INSERT QUESTION 2 INFO INTO QUESTIONS TRANSLATION DATABASE
        if question2:
            db.execute(
                """UPDATE question_translations SET (qnt_question, qnt_answer_1, qnt_answer_2, qnt_answer_3, qnt_answer_4, qnt_language_id) 
                = (?, ?, ?, ?, ?, ?) WHERE qnt_quiz_id = ? AND qnt_question_number = ?""", question2, question2_answer1, 
                question2_answer2, question2_answer3, question2_answer4, language, quiz_id, question2_number)

        # Question 3: 
        question3 = request.form.get("inputQNTQuestion3")
        if question3:
            question3_number = 3
        
        question3_answer1 = request.form.get("inputQNTAnswer1_Q3")
        question3_answer2 = request.form.get("inputQNTAnswer2_Q3")
        question3_answer3 = request.form.get("inputQNTAnswer3_Q3")
        question3_answer4 = request.form.get("inputQNTAnswer4_Q3")
        
        # INSERT QUESTION 3 INFO INTO QUESTIONS TRANSLATION DATABASE
        if question3:
            db.execute(
                """UPDATE question_translations SET (qnt_question, qnt_answer_1, qnt_answer_2, qnt_answer_3, qnt_answer_4, qnt_language_id) 
                = (?, ?, ?, ?, ?, ?) WHERE qnt_quiz_id = ? AND qnt_question_number = ?""", question3, question3_answer1, 
                question3_answer2, question3_answer3, question3_answer4, language, quiz_id, question3_number)                 

        # Display the quizzes in the database for exit
        rows = db.execute(
            """SELECT quiz_id, course_title, section_number, lesson_number, lesson_title, quiz_number, quiz_published 
            FROM quiz JOIN lessons ON quiz.quiz_lesson_id = lessons.lesson_id 
            JOIN sections ON lessons.lesson_section_id = sections.section_id 
            JOIN courses ON sections.section_course_id = courses.course_id 
            WHERE quiz_lesson_id = ? ORDER BY course_title ASC, section_number ASC, lesson_number ASC, quiz_number ASC""", quiz_lesson_id)
    
        # print(f"....QUIZ edit CONFIRM - rows: {rows}")
        # print(f"....QUIZ edit CONFIRM - lesson_id: {lesson_id}")

        # return redirect("/quiz")
        return render_template("quiz.html", quizzes=rows, no_edit=flag, lesson_id=quiz_lesson_id)
    else:
        return apology("You do not have permission to access this page", 400)
    

# Fetch Quiz translation based on user's language
def get_quiz(quiz_id):
    user_lang = str(get_locale())
    if user_lang == 'fr':
        lang = 2
    else:
        lang = 1

    # Fetch the default quiz data
    quiz = db.execute("SELECT * FROM quiz WHERE quiz_id = ?", quiz_id)[0]

    # Fetch the translation based on the user's language
    translation = db.execute("SELECT * FROM quiz_translations WHERE qzt_quiz_id = ? AND qzt_language_id = ?", quiz_id, lang)
    if translation:
        translation = translation[0]
        return {
            'quiz_title': translation['qzt_title'], # Translatable fields
            'quiz_subtitle': translation['qzt_subtitle'],
            'quiz_number': quiz['quiz_number'], # Non-Translatable fields
            'quiz_published': quiz['quiz_published'], 
            'quiz_id': quiz['quiz_id'],
            'quiz_lesson_id': quiz['quiz_lesson_id']
        }
    else:
        return {
            'quiz_title': quiz['quiz_title'], 
            'quiz_subtitle': quiz['quiz_subtitle'],
            'quiz_number': quiz['quiz_number'],
            'quiz_published': quiz['quiz_published'], 
            'quiz_id': quiz['quiz_id'],
            'quiz_lesson_id': quiz['quiz_lesson_id']
        }
    

# Fetch Question 1 translation based on user's language
def get_question_1(quiz_id):
    user_lang = str(get_locale())
    if user_lang == 'fr':
        lang = 2
    else:
        lang = 1 

    # Fetch the default question 1 data
    if question_count(quiz_id) >= 1:
        questions_1 = db.execute("SELECT * FROM questions WHERE question_quiz_id = ?", quiz_id)[0]
        # print(f"...DEF QET QUESTION 1: {questions_1}")

        # Fetch the translation for Question 1 based on the user's language
        translation = db.execute("SELECT * FROM question_translations WHERE qnt_quiz_id = ? AND qnt_question_number = ? AND qnt_language_id = ?", quiz_id, 1, lang)
        if translation:
            translation = translation[0]
            return {
                'question': translation['qnt_question'], # Translatable fields
                'answer_1': translation['qnt_answer_1'],
                'answer_2': translation['qnt_answer_2'],
                'answer_3': translation['qnt_answer_3'],
                'answer_4': translation['qnt_answer_4'],
                'question_number': questions_1['question_number'], # Non-Translatable fields
                'correct_answer': questions_1['correct_answer'], 
                'question_quiz_id': questions_1['question_quiz_id'],
                'question_id': questions_1['question_id']
            }
        else:
            return {
                'question': questions_1['question'],
                'answer_1': questions_1['answer_1'],
                'answer_2': questions_1['answer_2'],
                'answer_3': questions_1['answer_3'],
                'answer_4': questions_1['answer_4'],
                'question_number': questions_1['question_number'],
                'correct_answer': questions_1['correct_answer'], 
                'question_quiz_id': questions_1['question_quiz_id'],
                'question_id': questions_1['question_id']
            }
        

# Fetch Question 2 translation based on user's language
def get_question_2(quiz_id):
    user_lang = str(get_locale())
    if user_lang == 'fr':
        lang = 2
    else:
        lang = 1 
    # Fetch the default question data
    if question_count(quiz_id) > 1:
        questions_2 = db.execute("SELECT * FROM questions WHERE question_quiz_id = ?", quiz_id)[1]

        # Fetch the translation for Question 2 based on the user's language
        translation = db.execute("SELECT * FROM question_translations WHERE qnt_quiz_id = ? AND qnt_question_number = ? AND qnt_language_id = ?", quiz_id, 2, lang)
        if translation:
            translation = translation[0]
            return {
                'question': translation['qnt_question'], # Translatable fields
                'answer_1': translation['qnt_answer_1'],
                'answer_2': translation['qnt_answer_2'],
                'answer_3': translation['qnt_answer_3'],
                'answer_4': translation['qnt_answer_4'],
                'question_number': questions_2['question_number'], # Non-Translatable fields
                'correct_answer': questions_2['correct_answer'], 
                'question_quiz_id': questions_2['question_quiz_id'],
                'question_id': questions_2['question_id']
            }
        else:
            return {
                'question': questions_2['question'],
                'answer_1': questions_2['answer_1'],
                'answer_2': questions_2['answer_2'],
                'answer_3': questions_2['answer_3'],
                'answer_4': questions_2['answer_4'],
                'question_number': questions_2['question_number'],
                'correct_answer': questions_2['correct_answer'], 
                'question_quiz_id': questions_2['question_quiz_id'],
                'question_id': questions_2['question_id']
            }
        

# Fetch Question 2 translation based on user's language
def get_question_3(quiz_id):
    user_lang = str(get_locale())
    if user_lang == 'fr':
        lang = 2
    else:
        lang = 1 
    # Fetch the default question data
    if question_count(quiz_id) > 2:
        questions_3 = db.execute("SELECT * FROM questions WHERE question_quiz_id = ?", quiz_id)[2]

        # Fetch the translation for 1 Question based on the user's language
        translation = db.execute("SELECT * FROM question_translations WHERE qnt_quiz_id = ? AND qnt_question_number = ? AND qnt_language_id = ?", quiz_id, 3, lang)
        if translation:
            translation = translation[0]
            return {
                'question': translation['qnt_question'], # Translatable fields
                'answer_1': translation['qnt_answer_1'],
                'answer_2': translation['qnt_answer_2'],
                'answer_3': translation['qnt_answer_3'],
                'answer_4': translation['qnt_answer_4'],
                'question_number': questions_3['question_number'], # Non-Translatable fields
                'correct_answer': questions_3['correct_answer'], 
                'question_quiz_id': questions_3['question_quiz_id'],
                'question_id': questions_3['question_id']
            }
        else:
            return {
                'question': questions_3['question'],
                'answer_1': questions_3['answer_1'],
                'answer_2': questions_3['answer_2'],
                'answer_3': questions_3['answer_3'],
                'answer_4': questions_3['answer_4'],
                'question_number': questions_3['question_number'],
                'correct_answer': questions_3['correct_answer'], 
                'question_quiz_id': questions_3['question_quiz_id'],
                'question_id': questions_3['question_id']
            }
    

@app.route('/grade_quiz', methods=["GET", "POST"])
@login_required
def grade_quiz():

    # Check if Admin User
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role =  user_rows[0]['role_id']

    # Flag to show admin nav
    if user_role == 1:
        flag = True
    
    else:
        flag = False

    # Request quiz id from form
    quiz_id = request.form.get("grade_quiz_id")

    # Get lesson id from quiz id
    lesson = db.execute("SELECT quiz_lesson_id FROM quiz WHERE quiz_id = ?", quiz_id)
    if lesson:
        lesson_id = lesson[0]['quiz_lesson_id']

    # Request quiz responses from form
    question1_answer = request.form.get("question1_answer")
    if question1_answer:
        if question1_answer != '0':
            question1_answer = int(question1_answer)
        else:
            return apology("Please select an answer for question 1", 400)
    
    question2_answer = request.form.get("question2_answer")
    if question2_answer:
        if question2_answer != '0':
            question2_answer = int(question2_answer)
        else:
            return apology("Please select an answer for question 2", 400)

    question3_answer = request.form.get("question3_answer")
    if question3_answer:
        if question3_answer != '0':
            question3_answer = int(question3_answer)
        else:
            return apology("Please select an answer for question 3", 400)

    # Get number of questions in quiz
    count_number = db.execute("SELECT COUNT(question_number) FROM questions WHERE question_quiz_id = ?", quiz_id)
    if count_number:
        question_count = int(count_number[0]['COUNT(question_number)'])

    # Get correct answers from questions database
    # Question 1
    question1_info = db.execute(
        """SELECT question_quiz_id, question_number, correct_answer FROM questions WHERE question_quiz_id = ? AND question_number = ?""", quiz_id, 1)
    if question1_info:
        question1_correct = int(question1_info[0]['correct_answer'])

    # Question 2
    question2_info = db.execute(
        """SELECT question_quiz_id, question_number, correct_answer FROM questions WHERE question_quiz_id = ? AND question_number = ?""", quiz_id, 2)
    if question2_info:
        question2_correct = int(question2_info[0]['correct_answer'])

    # Question 3
    question3_info = db.execute(
        """SELECT question_quiz_id, question_number, correct_answer FROM questions WHERE question_quiz_id = ? AND question_number = ?""", quiz_id, 3)
    if question3_info:
        question3_correct = int(question3_info[0]['correct_answer'])

    # Grade quiz
    # 1 question scenario
    if question_count == 1:
        if question1_answer == question1_correct:
            grade = 100
        else:
            grade = 0
    
    # 2 question scenario
    if question_count == 2:
        count = 0
        if question1_answer == question1_correct:
            count += 1
        if question2_answer == question2_correct:
            count += 1

        if count == 1:
            grade = 50
        elif count == 2:
            grade = 100
        else:
            grade = 0

    # 3 question scenario
    if question_count == 3:
        count = 0
        if question1_answer == question1_correct:
            count += 1
        if question2_answer == question2_correct:
            count += 1
        if question3_answer == question3_correct:
            count += 1

        if count == 1:
            grade = 33
        elif count == 2:
            grade = 66
        elif count == 3:
            grade = 100
        else:
            grade = 0
        
    # TODO: Adjust when quiz start button configured on frontend
    now_utc = datetime.now(pytz.UTC)

    # Check if user already took test
    exists = db.execute("SELECT quiz_grade_id FROM quiz_grades WHERE qg_quiz_id = ? AND qg_user_id = ?", quiz_id, users_id)
    if exists:
        # Update quiz_grades
        db.execute("UPDATE quiz_grades SET (quiz_grade, quiz_completed_datetime) = (?, ?) WHERE qg_quiz_id = ? AND qg_user_id = ?", grade, now_utc, quiz_id, users_id)
    else:
        # Insert into quiz_grades
        db.execute(
            """INSERT INTO quiz_grades (quiz_grade, quiz_started_datetime, quiz_completed, qg_quiz_id, qg_user_id) 
            VALUES (?, ?, ?, ?, ?)""", grade, now_utc, 1, quiz_id, users_id)
    
    # Update Lesson Quiz Grades:

    # Get current lesson quiz grade first
    current_quiz = db.execute("SELECT lesson_quiz_grade FROM lesson_grades WHERE lg_lesson_id = ? AND lg_user_id = ?", lesson_id, users_id)
    if current_quiz:
        current_quiz_grade = current_quiz[0]['lesson_quiz_grade']
    # print(f"....GRADE QUIZ - Current Quiz: {current_quiz_grade}")

    # Check how many quizzes in lesson
    quiz_counting = db.execute("SELECT COUNT(quiz_number) FROM quiz WHERE quiz_lesson_id = ?", lesson_id)
    if quiz_counting:
        quiz_count = quiz_counting[0]['COUNT(quiz_number)']
        quiz_count = int(quiz_count)
    # print(f"....GRADE QUIZ - Quiz Count: {quiz_count}")


    # If one quiz, replace quiz score with new quiz score
    if quiz_count == 1:
        new_quiz_grade = grade

        # print(f"....GRADE QUIZ - 1 Quiz - New Grade: {new_quiz_grade}")

    # TODO: ensure this works after configuring multiple quizzes per lesson
    # If multiple quizzes, get average quiz score.
    if quiz_count > 1:
        if current_quiz_grade is not None:
            new_quiz_grade = int((current_quiz_grade + grade) / quiz_count)
        else:
            new_quiz_grade = grade

    # Update the lesson quiz grade (lesson grade data created before starting quiz)
    db.execute("UPDATE lesson_grades SET (lesson_quiz_grade) = (?) WHERE lg_lesson_id = ? AND lg_user_id = ?", new_quiz_grade, lesson_id, users_id)

    # Update Lesson grade
    # Check if HW grade, if not update lesson grade with quiz grade
    current_hw = db.execute("SELECT lesson_hw_grade FROM lesson_grades WHERE lg_lesson_id = ? AND lg_user_id = ?", lesson_id, users_id)

    # TODO: CONSTANT dependent on how many (quiz,hw) columns being averaged in lesson grade
    if current_hw:
        current_hw_grade = current_hw[0]['lesson_hw_grade']
        if current_hw_grade is not None:
            new_lesson_grade = int((current_hw_grade + new_quiz_grade)/2)
        else:
            new_lesson_grade = new_quiz_grade

    # Update the lesson grade
    db.execute("UPDATE lesson_grades SET (lesson_grade) = (?) WHERE lg_lesson_id = ? AND lg_user_id = ?", new_lesson_grade, lesson_id, users_id)
    
    return redirect(f'/lesson_detail/{lesson_id}')

# """ ----------------------- QUIZ END ----------------------- ""


# """ ----------------------- FRENCH START ----------------------- ""

@app.route('/fr_login', methods=["GET", "POST"])
def fr_login():
    return render_template("fr_login.html")


@app.route('/fr_reset_request', methods=['GET', 'POST'])
def fr_reset_request():
    if request.method == 'POST':
        email = request.form.get('email')
        user = db.execute("SELECT email, id FROM users WHERE email = :email", email=email)
        if user:
            user_id = user[0]['id']
            language = db.execute("SELECT user_language_id FROM user_profiles WHERE profile_id = ?", user_id)
            if language:
                lang_pref = language[0]['user_language_id']
                if lang_pref == 2:
                    token = fr_generate_reset_token(email)
                    fr_send_reset_email(email, token)
                else:
                    return redirect(url_for('reset_request'))
            flash('Un e-mail a été envoyé avec des instructions pour réinitialiser votre mot de passe.', 'info')
            return redirect(url_for('fr_login'))
        else:
            flash('Adresse e-mail introuvable.', 'warning')
    return render_template('fr_reset_request.html')


# French Reset password email
def fr_send_reset_email(to_email, token):
    message = Message('Demande de réinitialisation du mot de passe',
                 recipients=[to_email])
    message.html = f'''Pour réinitialiser votre mot de passe, visitez le lien suivant:
    <a href='{url_for('fr_reset_token', token=token, _external=True)}'>{url_for('fr_reset_token', token=token, _external=True)}</a><br>

    Si vous n'avez pas fait cette demande, ignorez simplement cet e-mail et aucune modification ne sera apportée.
    '''
    mail.send(message)


# French Token for resetting password
def fr_generate_reset_token(email):
    # from itsdangerous import URLSafeTimedSerializer
    serializer = URLSafeTimedSerializer(app.config['SECRET_KEY'])
    return serializer.dumps(email, salt='password-reset-salt')


# French Route for password reset form
@app.route('/fr_reset_password/<token>', methods=['GET', 'POST'])
def fr_reset_token(token):
    # from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature
    try:
        serializer = URLSafeTimedSerializer(app.config['SECRET_KEY'])
        email = serializer.loads(token, salt='password-reset-salt', max_age=3600)  # Token valid for 1 hour
    except (SignatureExpired, BadSignature):
        flash("Le lien de réinitialisation du mot de passe n'est pas valide ou a expiré.", 'warning')
        return redirect(url_for('fr_reset_request'))

    if request.method == 'POST':
        password = request.form.get('password')
        confirmation = request.form.get('confirmation')
        if password != confirmation:
            return apology("Les mots de passe ne correspondent pas", 400)
            # flash('Passwords do not match, please re-enter your password and confirmation', 'warning')
        if password:
            hashed_password = generate_password_hash(password, method='pbkdf2:sha256:600000', salt_length=16)

            # Update new password into users database
            db.execute("UPDATE users SET hash = :hash WHERE email = :email", hash=hashed_password, email=email)
            flash('Votre mot de passe a été mis à jour!', 'success')

            return redirect(url_for('fr_login'))
            
    return render_template('fr_reset_password.html')

# """ ----------------------- FRENCH END ----------------------- ""

# """ ----------------------- PROGRAM START ----------------------- ""

@app.route("/programs", methods=["GET", "POST"])
@login_required
def programs():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for edit button template
        flag = True

        # Display the programs in the database 
        rows = db.execute(
            """SELECT program_id, program, program_start_date, program_end_date, program_published, program_coordinator_id,
            first_name, last_name, id 
            FROM programs JOIN users ON program_coordinator_id = users.id""")
        # print(f"....PROGRAMS - ROWS: {rows}")

        return render_template("programs.html", programs=rows, no_edit=flag)
    
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/program_edit", methods=["POST"])
@login_required
def program_edit():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for program edit button
        flag = True

        # Select Program to edit
        id = request.form.get("program_edit_id")
        if id:

            # Display the program info from the database 
            rows = db.execute(
                """SELECT program_id, program, program_title, program_subtitle, program_overview, program_picture, 
                program_video, program_start_date, program_end_date, program_coordinator_id, program_published, 
                first_name, last_name, id 
                FROM programs JOIN users on programs.program_coordinator_id = users.id WHERE program_id = ?""", id)
            # print(f"...PROGRAM EDIT - ROWS:{rows}")
            
            # Select all fields to populate in form-select types
            courses = db.execute("SELECT course_id, course_title, language_id FROM courses")
            coordinators = db.execute("SELECT id, first_name, last_name, role_id FROM users WHERE role_id = ?", 3)
            # print(f"...PROGRAM EDIT - Courses:{courses}")
            # print(f"...PROGRAM EDIT - Coordinators:{coordinators}")
                
            return render_template("programs.html", yes_edit=flag, programs=rows, courses=courses, coordinators=coordinators, edit_program_id=id)
        
        else:
            return apology("Program not found, Please define program id", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/program_edit_confirm", methods=["POST"])
@login_required
def program_edit_confirm():

     # Ensure valid admin user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Select Program to edit
        id = request.form.get("program_edit_id")

        if id:
            # Verify and upload picture first
            file = request.files.get('programPicUpload')
            # print(f"...File: {file}")

            # If upload, picture is named secure filename
            if file and file.filename != '':
                picture = upload_img(file, app.config['ALLOWED_IMAGE_EXTENSIONS'])
                # print(f"...PROGRAM EDIT CONFIRM - Picture: {picture}")
                if not picture:
                    return apology("File upload failed or file is not allowed", 400)
            
            else:
                # If no upload, pic is set to current or default pic
                picture = request.form.get("currentProgramPic")
                # print(f"...PROGRAM EDIT CONFIRM - Picture (no upload): {picture}")
                if not picture or picture == 'None':
                    picture = 'course-img.png'
                    # print(f"...PROGRAM EDIT CONFIRM - Picture (default): {picture}")

            # Verify and upload Published
            published = request.form.get("inputProgramPublished")
            if published:
                published = int(published)

            # Verify start and end 
            start = request.form.get("inputProgramStart")
            if not start:
                start = None
            end = request.form.get("inputProgramEnd")
            if not end:
                end = None

            # Get new values for each variable in program (Ensure foreign keys have value=id)
            title = request.form.get("inputProgramPlaceTitle")
            subtitle = request.form.get("inputProgramSubtitle")
            overview = request.form.get("inputProgramOverview")
            coordinator = request.form.get("inputProgramCoordinator")
            
            # Ensure video is embed link
            video = request.form.get("inputProgramVideo")
            if video and video != 'None':
                if 'https://www.youtube.com/embed/' not in video:
                    return apology("Youtube link must be from Embed link", 400)
                elif '"' in video:
                    return apology("Youtube link cannot include quotations", 400)
            
            #  Update each field in programs
            db.execute(
                """UPDATE programs SET (program_title, program_subtitle, program_overview, program_picture, program_video, program_start_date, 
                program_end_date, program_coordinator_id, program_published) = (?, ?, ?, ?, ?, ?, ?, ?, ?) WHERE program_id = ?""", 
                title, subtitle, overview, picture, video, start, end, coordinator, published, id)

            # return render_template("courses.html", courses=rows, no_edit=flag)
            return redirect("/programs")
        
        else:
            return apology("Program not found, Please define program id", 400)
        
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/program_translate", methods=["POST"])
@login_required
def program_translate():

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for program edit button
        flag = True

        # Select Program to edit
        id = request.form.get("program_translate_id")
        if id:

            # Display the program info from the database 
            rows = db.execute(
                """SELECT program_id, program, program_title, program_subtitle, program_overview, program_picture, program_video 
                FROM programs WHERE program_id = ?""", id)
            # print(f"...PROGRAM TRANSLATE - ROWS:{rows}")
            
            # Select all fields to populate in form-select types
            languages = db.execute("SELECT language_id, language FROM languages")
            # print(f"...PROGRAM TRANSLATE  - Languages:{language}")

            # Check if current translations to show
            translations = db.execute(
                """SELECT * FROM program_translations WHERE pt_program_id = ?""", id)

            if translations:
                # print(f"...PROGRAM TRANSLATE  - Translations:{translations}")

                # Get translation id
                pt_id = translations[0]['pt_id']
            else:
                # Insert into translations and get info
                db.execute("INSERT INTO program_translations (pt_program_id) VALUES (?)", id)
                translations = db.execute("SELECT * FROM program_translations WHERE pt_program_id = ?", id)
                pt_id = translations[0]['pt_id']

            return render_template("programs.html", yes_edit_translate=flag, programs=rows, 
                                   translations=translations, languages=languages, edit_program_id=id, pt_id=pt_id)
        
        else:
            return apology("Program not found, Please define program id", 400)
    else:
        return apology("You do not have permission to access this page", 400)
    

@app.route("/program_translate_confirm", methods=["POST"])
@login_required
def program_translate_confirm():

     # Ensure valid admin user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Select Program
        id = request.form.get("program_edit_id")
        # Select Translation
        pt_id = request.form.get("program_translation_id")

        if id:
            # Verify and upload picture first
            file = request.files.get('ptPicUpload')
            # print(f"...File: {file}")

            # If upload, picture is named secure filename
            if file and file.filename != '':
                picture = upload_img(file, app.config['ALLOWED_IMAGE_EXTENSIONS'])
                # print(f"...PROGRAM EDIT CONFIRM - Picture: {picture}")
                if not picture:
                    return apology("File upload failed or file is not allowed", 400)
            else:
                # If no upload, pic is set to current or default pic
                picture = request.form.get("currentPTPic")
                # print(f"...PROGRAM EDIT CONFIRM - Picture (no upload): {picture}")
                if not picture or picture == 'None':
                    picture = 'course-img.png'
                    # print(f"...PROGRAM EDIT CONFIRM - Picture (default): {picture}")

            # Get new values for each variable in program (Ensure foreign keys have value=id)
            title = request.form.get("inputPTPlaceTitle")
            subtitle = request.form.get("inputPTSubtitle")
            overview = request.form.get("inputPTOverview")
            language = request.form.get("inputPTLanguage")
            # print(f"...PROGRAM TRANSLATE CONFIRM - Language: {language}")
            
            # Ensure video is embed link
            video = request.form.get("inputPTVideo")
            if video:
                if 'https://www.youtube.com/embed/' not in video:
                    return apology("Youtube link must be from Embed link", 400)
                elif '"' in video:
                    return apology("Youtube link cannot include quotations", 400)
            
            #  Update each field in program translation
            db.execute(
                """UPDATE program_translations SET (pt_title, pt_subtitle, pt_overview, pt_picture, pt_video, pt_language_id) = 
                (?, ?, ?, ?, ?, ?) WHERE pt_program_id = ?""", 
                title, subtitle, overview, picture, video, language, id)

            # return render_template("courses.html", courses=rows, no_edit=flag)
            return redirect("/programs")
        
        else:
            return apology("Program not found, Please define program id", 400)
        
    else:
        return apology("You do not have permission to access this page", 400)


# Fetch translation based on user's language
def get_program(program_id):
    user_lang = str(get_locale())

    if user_lang == 'fr':
        lang = 2
    else:
        lang = 1

    # Fetch the default program data
    program = db.execute("SELECT * FROM programs WHERE program_id = ?", program_id)[0]

    # Fetch the translation based on the user's language
    translation = db.execute("SELECT * FROM program_translations WHERE pt_program_id = ? AND pt_language_id = ?", program_id, lang)
    if translation:
        translation = translation[0]
        return {
            'program_title': translation['pt_title'], # Translatable fields
            'program_subtitle': translation['pt_subtitle'],
            'program_overview': translation['pt_overview'],
            'program_picture': translation['pt_picture'],
            'program_video': translation['pt_video'],
            'program_start_date': program['program_start_date'], # Non-Translatable fields
            'program_end_date': program['program_end_date']
        }
    else:
        return {
            'program_title': program['program_title'],
            'program_subtitle': program['program_subtitle'],
            'program_overview': program['program_overview'],
            'program_picture': program['program_picture'],
            'program_video': program['program_video'],
            'program_start_date': program['program_start_date'],
            'program_end_date': program['program_end_date']
        }


@app.route('/program_detail/<int:program_id>', methods=["POST"])
@login_required
def program_detail(program_id):
    program = get_program(program_id)

    # Check if Admin User
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role =  user_rows[0]['role_id']
    if user_role == 1:
        flag = True
    else:
        flag = False

    return render_template('program_detail.html', program=program, admin=flag)


@app.route("/program_courses", methods=["GET", "POST"])
@login_required
def program_courses():
    """Show Dashboard"""

    # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for edit button template
        flag = True

        program = request.form.get("program_id")

        # Display the courses in the database 
        rows = db.execute(
            """SELECT course_id, course_title, course_subtitle, course_capacity, instructor_id, language_id, course_published, 
            courses.level_id, level, first_name, last_name
            FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN users ON users.id = courses.instructor_id""")
        # print(f"....PROGRAMS - ROWS: {rows}")

        # Display the courses in the program courses database 
        new_rows = db.execute(
            """SELECT course_id, course_title, course_subtitle, course_capacity, instructor_id, language_id, course_published, 
            courses.level_id, level, first_name, last_name, pc_id, pc_program_id, pc_language_id, pc_course_number, pc_course_id, program
            FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN users ON users.id = courses.instructor_id 
            JOIN program_courses ON courses.course_id = program_courses.pc_course_id JOIN programs ON program_courses.pc_program_id =
            programs.program_id WHERE pc_program_id = ? ORDER BY pc_course_number""", program)
        if new_rows:
            program_name = new_rows[0]['program']
        else:
            program_name = ''
        # print(f"....PROGRAMS - ROWS: {rows}")

        return render_template("programs.html", results=rows, courses=new_rows, program_id=program, program_name=program_name, yes_add_course=flag)
    
    else:
        return apology("You do not have permission to access this page", 400)


# Search courses in program
@app.route("/program_search_courses")
def search():
    q = request.args.get('q', '')

    query = """
        SELECT * FROM courses JOIN levels ON courses.level_id = levels.level_id 
        JOIN users ON courses.instructor_id = users.id
        """
    
    if q:
        query += """
        WHERE course_title LIKE :q OR course_subtitle LIKE :q OR course_overview LIKE :q
        """
    query += """
    ORDER BY course_title ASC 
    LIMIT 100
    """
    results = db.execute(query, q=f'%{q}%' if q else '%')

    return render_template("program_search_courses.html", results=results)


# Add course to program
@app.route("/program_course_add", methods=["GET", "POST"])
def program_course_add():
    try:
        app.logger.info('Program course add route accessed')

        # Ensure valid user
        users_id = session["user_id"]
        app.logger.info(f'User id: {users_id}')
        user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
        user_role = user_rows[0]['role_id']
        app.logger.info(f'User role: {user_role}')

        if user_role == 1:

            # Flag for edit button template
            flag = True

            program = request.form.get("program_id")
            course = request.form.get("course_id")
            app.logger.info(f'Program id: {program}, Course id: {course}')

            if course:
                # Check language of course:
                course_language = db.execute("SELECT language_id FROM courses WHERE course_id = ?", course)[0]['language_id']
                app.logger.info(f'Course language: {course_language}')

            # Check how many courses in program, update program course count
            count = db.execute("SELECT COUNT(pc_course_id) FROM program_courses WHERE pc_program_id = ?", program)
            # print(f"....PROGRAM COURSE ADD - COUNT: {count}")
            app.logger.info(f'Program course count: {count}')

            count = count[0]['COUNT(pc_course_id)']
            if count:
                # Get course number in program
                program_info = db.execute("SELECT pc_course_number FROM program_courses WHERE pc_program_id = ? ORDER BY pc_course_number", program)
                if count == 0:
                    latest_count = 0
                elif count == 1:
                    latest_count = 1
                else:
                    latest_count = program_info[-1]['pc_course_number']
            else:
                latest_count = 0
            app.logger.info(f'Latest course count: {latest_count}')
            
            # Ensure no duplicate course added to program:
            duplicate = db.execute("SELECT pc_course_id FROM program_courses WHERE pc_course_id = ? AND pc_program_id = ?", course, program)
            if not duplicate:
                # Insert new course, language, number into program
                db.execute(
                    """INSERT INTO program_courses (pc_program_id, pc_course_id, pc_course_number, pc_language_id) VALUES 
                    (?, ?, ?, ?)""", program, course, latest_count + 1, course_language)
                app.logger.info(f'Course {course} added to program {program}')
            else:
                flash('Course already added.', 'warning')

            # Display the courses in the database 
            rows = db.execute(
                """SELECT course_id, course_title, course_subtitle, course_capacity, instructor_id, language_id, course_published, 
                courses.level_id, level, first_name, last_name
                FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN users ON users.id = courses.instructor_id""")

            # Display the courses in the program courses database 
            new_rows = db.execute(
                """SELECT course_id, course_title, course_subtitle, course_capacity, instructor_id, language_id, course_published, 
                courses.level_id, level, first_name, last_name, pc_id, pc_program_id, pc_language_id, pc_course_number, pc_course_id, program
                FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN users ON users.id = courses.instructor_id 
                JOIN program_courses ON courses.course_id = program_courses.pc_course_id JOIN programs ON program_courses.pc_program_id =
                programs.program_id WHERE pc_program_id = ? ORDER BY pc_course_number""", program)
            if new_rows:
                program_name = new_rows[0]['program']
            else:
                program_name = ''
            app.logger.info(f'Program name: {program_name}')

            return render_template("programs.html", results=rows, courses=new_rows, program_id=program, program_name=program_name, yes_add_course=flag)
        else:
                app.logger.info('User does not have permission to access this page')
                return apology("You do not have permission to access this page", 400)
    except Exception as e:
        app.logger.error(f'Error in program_course_add: {e}')
        return apology("An error occurred", 500)


# Add course to program
@app.route("/program_course_remove", methods=["GET", "POST"])
def program_course_remove():

     # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for edit button template
        flag = True

        program = request.form.get("program_id")
        course = request.form.get("course_id")

        # Ensure course in program_courses table
        check = db.execute("SELECT pc_course_id FROM program_courses WHERE pc_course_id = ? AND pc_program_id = ?", course, program)
        if check:
            # Remove course from program
            db.execute("DELETE FROM program_courses WHERE pc_program_id = ? AND pc_course_id = ?", program, course)
        else:
            flash('Course already removed.', 'warning')

        # Update Course Numbers (AUTOMATIC BASED ON COUNT OF COURSES IN PROGRAM)
        course_list = db.execute("SELECT pc_course_id, pc_program_id, pc_course_number FROM program_courses WHERE pc_program_id = ? ORDER BY pc_course_number", program)
        # print(f"....QUIZ EDIT CONFIRM - Quiz_List: {quiz_list}")

        if course_list:
            # sort course_list by 'course_number'
            course_list.sort(key=lambda x: x['pc_course_number'])

            # iterate over sorted list
            for i, course_ in enumerate(course_list):
                # update 'course_number' to match position in list
                course_['pc_course_number'] = i + 1
                db.execute("UPDATE program_courses SET (pc_course_number) = (?) WHERE pc_course_id = ?", i + 1, course_['pc_course_id'])

        # Display the courses in the database 
        rows = db.execute(
            """SELECT course_id, course_title, course_subtitle, course_capacity, instructor_id, language_id, course_published, 
            courses.level_id, level, first_name, last_name
            FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN users ON users.id = courses.instructor_id""")

        # Display the courses in the program courses database 
        new_rows = db.execute(
            """SELECT course_id, course_title, course_subtitle, course_capacity, instructor_id, language_id, course_published, 
            courses.level_id, level, first_name, last_name, pc_id, pc_program_id, pc_language_id, pc_course_number, pc_course_id, program
            FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN users ON users.id = courses.instructor_id 
            JOIN program_courses ON courses.course_id = program_courses.pc_course_id JOIN programs ON program_courses.pc_program_id =
            programs.program_id WHERE pc_program_id = ? ORDER BY pc_course_number""", program)
        if new_rows:
            program_name = new_rows[0]['program']
        else:
            program_name = ''
        # print(f"....PROGRAM COURSE ADD - ROWS: {new_rows}")

        return render_template("programs.html", results=rows, courses=new_rows, program_id=program, program_name=program_name, yes_add_course=flag)
    else:
        return apology("You do not have permission to access this page", 400)


# Edit course in program
@app.route("/program_course_edit", methods=["GET", "POST"])
def program_course_edit():

     # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for edit button template
        flag = True

        program = request.form.get("program_id")
        course = request.form.get("course_id")

        course_title = db.execute("SELECT course_title FROM courses WHERE course_id = ?", course)
        if course_title:
            course_title = course_title[0]['course_title']

        # Display the courses in the program courses database 
        new_rows = db.execute(
            """SELECT course_id, course_title, course_subtitle, course_capacity, instructor_id, language_id, course_published, 
            courses.level_id, level, first_name, last_name, pc_id, pc_program_id, pc_language_id, pc_course_number, pc_course_id, program
            FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN users ON users.id = courses.instructor_id 
            JOIN program_courses ON courses.course_id = program_courses.pc_course_id JOIN programs ON program_courses.pc_program_id =
            programs.program_id WHERE pc_program_id = ? ORDER BY pc_course_number""", program)
        if new_rows:
            program_name = new_rows[0]['program']
        else:
            program_name = ''
        # print(f"....PROGRAM COURSE ADD - ROWS: {new_rows}")

        return render_template("programs.html", courses=new_rows, program_id=program, program_name=program_name, course_title=course_title,
                               edit_course_id=course, yes_edit_course=flag)
    else:
        return apology("You do not have permission to access this page", 400)
    

# Edit course number in program
@app.route("/program_course_edit_confirm", methods=["GET", "POST"])
def program_course_edit_confirm():

     # Ensure valid user
    users_id = session["user_id"]
    user_rows = db.execute("SELECT role_id FROM users WHERE id = ?", users_id)
    user_role = user_rows[0]['role_id']

    if user_role == 1:

        # Flag for edit button template
        flag = True

        program = request.form.get("program_edit_id")
        edit_course = request.form.get("edit_course_id")
        switch_course = request.form.get("switch_course_id")

        if not switch_course:
            return apology("Please select a course to switch", 400)
        
        # Get edit course number
        edit_info = db.execute("SELECT pc_course_number FROM program_courses WHERE pc_program_id = ? AND pc_course_id = ?", program, edit_course)
        # print(f"....PROGRAM COURSE EDIT CONFIRM - edit_info: {edit_info}")
        if edit_info:
            edit_course_number = edit_info[0]['pc_course_number']

        # Get switch course number
        switch_info = db.execute("SELECT pc_course_number FROM program_courses WHERE pc_program_id = ? AND pc_course_id = ?", program, switch_course)
        # print(f"....PROGRAM COURSE EDIT CONFIRM - switch_info: {switch_info}")
        if switch_info:
            switch_course_number = switch_info[0]['pc_course_number']

        # Change edit course number to switch
        db.execute(
            """UPDATE program_courses SET (pc_course_number) = (?) WHERE pc_course_id = ? AND pc_program_id = ?""", switch_course_number, edit_course, program)

        # Change switch course number to edit
        db.execute(
            """UPDATE program_courses SET (pc_course_number) = (?) WHERE pc_course_id = ? AND pc_program_id = ?""", edit_course_number, switch_course, program)
        
        # Display the courses in the database 
        rows = db.execute(
            """SELECT course_id, course_title, course_subtitle, course_capacity, instructor_id, language_id, course_published, 
            courses.level_id, level, first_name, last_name
            FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN users ON users.id = courses.instructor_id""")
        
        # Display the courses in the program courses database 
        new_rows = db.execute(
            """SELECT course_id, course_title, course_subtitle, course_capacity, instructor_id, language_id, course_published, 
            courses.level_id, level, first_name, last_name, pc_id, pc_program_id, pc_language_id, pc_course_number, pc_course_id, program
            FROM courses JOIN levels ON courses.level_id = levels.level_id JOIN users ON users.id = courses.instructor_id 
            JOIN program_courses ON courses.course_id = program_courses.pc_course_id JOIN programs ON program_courses.pc_program_id =
            programs.program_id WHERE pc_program_id = ? ORDER BY pc_course_number""", program)
        if new_rows:
            program_name = new_rows[0]['program']
        else:
            program_name = ''
        # print(f"....PROGRAM COURSE ADD - ROWS: {new_rows}")

        return render_template("programs.html", results=rows, courses=new_rows, program_id=program, program_name=program_name,
                               edit_course_id=edit_course, yes_add_course=flag)
    else:
        return apology("You do not have permission to access this page", 400)


# """ ----------------------- PROGRAM END ----------------------- ""

# Project Intro
@app.route("/intro", methods=["GET"])
def intro():
    return render_template("intro.html")