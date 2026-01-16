from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

# Dictionary containing Hindi songs categorized by mood
songs_by_mood = {
    "Happy": ["Gulabi Aankhen - Mohit Chauhan", "Badtameez Dil - Ranbir Kapoor", "Senorita - Joiita Gandhi"],
    "Sad": ["Tu Jaane Na - Atif Aslam", "Channa Mereya - Arijit Singh", "Bheege Honth Tere - KK"],
    "Relaxed": ["Phir Le Aaya Dil - Arijit Singh", "Shaam - Mohit Chauhan", "Dil Dhoondta Hai - Kishore Kumar"],
    "Energetic": ["Malhari - Vishal Dadlani", "Zinda - Shankar Mahadevan", "Jai Jai Shivshankar - Vishal-Shekhar"],
    "Romantic": ["Tum Hi Ho - Arijit Singh", "Tera Ban Jaunga - Tulsi Kumar", "Pehla Nasha - Udit Narayan"]
}

@app.route('/')
def home():
    return render_template("index.html")

@app.route('/get_songs', methods=['POST'])
def get_songs():
    data = request.get_json()
    mood = data.get("mood", "")

    # Fetch songs based on mood, default to an empty list if mood is not found
    songs = songs_by_mood.get(mood, [])

    return jsonify(songs)

if __name__ == '__main__':
    app.run(debug=True)

