import os
from flask import Flask, request, jsonify, send_file, render_template
from flask_sqlalchemy import SQLAlchemy
from werkzeug.utils import secure_filename
from datetime import datetime
import magic

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024  # 32MB max file size
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///music.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# Ensure upload directory exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

class Song(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    title = db.Column(db.String(255))
    artist = db.Column(db.String(255))
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow)
    file_path = db.Column(db.String(255), nullable=False)

    def to_dict(self):
        return {
            'id': self.id,
            'filename': self.filename,
            'title': self.title,
            'artist': self.artist,
            'uploaded_at': self.uploaded_at.isoformat(),
        }

def allowed_file(filename):
    mime = magic.Magic(mime=True)
    return mime.from_file(filename).startswith('audio/')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/songs', methods=['GET'])
def get_songs():
    songs = Song.query.all()
    return jsonify([song.to_dict() for song in songs])

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'files[]' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    files = request.files.getlist('files[]')
    uploaded_files = []
    
    for file in files:
        if file.filename == '':
            continue
            
        if file:
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            
            if not allowed_file(file_path):
                os.remove(file_path)
                continue
                
            song = Song(
                filename=filename,
                title=os.path.splitext(filename)[0],
                file_path=file_path
            )
            db.session.add(song)
            uploaded_files.append(filename)
    
    db.session.commit()
    return jsonify({'message': f'Successfully uploaded {len(uploaded_files)} files', 'files': uploaded_files})

@app.route('/api/stream/<int:song_id>')
def stream_file(song_id):
    song = Song.query.get_or_404(song_id)
    return send_file(song.file_path)

@app.route('/api/delete/<int:song_id>', methods=['DELETE'])
def delete_song(song_id):
    song = Song.query.get_or_404(song_id)
    try:
        os.remove(song.file_path)
    except OSError:
        pass
    db.session.delete(song)
    db.session.commit()
    return jsonify({'message': 'Song deleted successfully'})

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(host='0.0.0.0', port=5000) 