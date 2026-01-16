import os
from flask import Flask, render_template, redirect, request
from io import BytesIO
from PIL import ImageGrab
import pyperclip
import base64
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from sqlalchemy.sql import exists

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'

# Connect to the database
# get the directory containing the executable file
basedir = os.path.abspath(os.path.dirname(__file__))

# set the path to the database file relative to the directory containing the executable file
db_path = os.path.join(basedir, 'db.sqlite3')

# set the database URI using the absolute path to the database file
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + db_path

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Create a model
class ClipboardData(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.Text)
    type = db.Column(db.String(10))
    date_added = db.Column(db.DateTime, default=datetime.utcnow)

@app.route('/')
def index():
    db.create_all()
    db.session.commit()
    return render_template('index.html')

@app.route('/clear')
def clear():
    # Clear the database and refresh the page
    db.drop_all()
    db.create_all()
    db.session.commit()
    return redirect('/')

@app.route('/update-data')
def update_data():
    # Retrieve data from sqlite db and send to template
    data = ClipboardData.query.all()
    return render_template('clipboard.html', DATA=data)

@app.route('/add', methods=['POST'])
def add():
    clipboard_data = request.form.get('message-form')
    print(clipboard_data)
    if not clipboard_data:
        return redirect('/')
    else:
        # Insert a record
        if not db.session.query(exists().where(ClipboardData.data == clipboard_data)).scalar():
            # Data doesn't exist, so add it to the database
            data = ClipboardData(data=clipboard_data, type="text")
            db.session.add(data)
            db.session.commit()
        # RETURN REDIRECT TO INDEX
        return redirect('/')

@app.route('/refresh-clipboard')
def refresh_clipboard():
    clipboard_data = None

    # Try to get text data from clipboard
    clipboard_data = pyperclip.paste()

    if not clipboard_data:
        clipboard_data = None

    # If no text data, try to get image data from clipboard
    if clipboard_data is None:
        try:
            image = ImageGrab.grabclipboard()

            if image is not None:
                buffer = BytesIO()
                image_format = image.format.lower()

                if image_format == 'jpeg':
                    image.save(buffer, format='JPEG')
                else:
                    image.save(buffer, format='PNG')

                clipboard_data = buffer.getvalue()
        except Exception:
            pass

    # If no data in clipboard, return an empty response
    if clipboard_data is None:
       # RETURN REDIRECT TO INDEX
        return redirect('/update-data')
    
    # Return the clipboard data
    if isinstance(clipboard_data, bytes):
        # Return the clipboard data as a base64-encoded string
        clipboard_data = base64.b64encode(clipboard_data).decode('utf-8')
        # Insert a record
        if not db.session.query(exists().where(ClipboardData.data == clipboard_data)).scalar():
            # Data doesn't exist, so add it to the database
            data = ClipboardData(data=clipboard_data, type="image")
            db.session.add(data)
            db.session.commit()
        # RETURN REDIRECT TO INDEX
        return redirect('/update-data')
    
    else:
        # Return the clipboard data as text
        # Insert a record
        if not db.session.query(exists().where(ClipboardData.data == clipboard_data)).scalar():
            # Data doesn't exist, so add it to the database
            data = ClipboardData(data=clipboard_data, type="text")
            db.session.add(data)
            db.session.commit()
        # RETURN REDIRECT TO INDEX
        return redirect('/update-data')

if __name__=='__main__':
    app.run(debug=True, host="0.0.0.0")