from flask import Flask, render_template, request, redirect, url_for, session
from pymongo import MongoClient
import os


mongo_uri = os.getenv('MONGODB_URI')
client    = MongoClient(mongo_uri)


app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), 'templates')
)
app.secret_key = os.getenv('SECRET_KEY', '1234567890')

# MongoDB Atlas Connection
mongo_uri = os.getenv(
    'MONGODB_URI',
    'mongodb+srv://gorkemerrr55:rSli7CuYAF9h0Dx2@gameplatform.ik3xjkw.mongodb.net/?retryWrites=true&w=majority'
)
client = MongoClient(mongo_uri)
db = client['game_platform']
users_col = db['users']
games_col = db['games']

# Create Indexes for Performance
games_col.create_index([("name", 1)])
users_col.create_index([("name", 1)])


# Home Page (alias at both '/' and '/home')
@app.route('/')
@app.route('/home')
def home():
    if 'username' not in session:
        return redirect(url_for('login'))
    games = games_col.find().sort("name", 1)
    users = users_col.find().sort("name", 1)
    return render_template('home.html', games=games, users=users)


@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))


@app.route('/user/<username>')
def user_profile(username):
    if 'username' not in session:
        return redirect(url_for('login'))
    user = users_col.find_one({"name": username})
    if not user:
        return "Kullanıcı bulunamadı", 404

    ratings = user.get('ratings', [])
    avg_rating = sum(r['value'] for r in ratings) / len(ratings) if ratings else None
    play_times = user.get('play_times', {})
    most_played = max(play_times, key=play_times.get) if play_times else None

    return render_template(
        'user.html',
        user=user,
        most_played=most_played,
        avg_rating=avg_rating,
        is_admin=False
    )


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        user = users_col.find_one({"name": username})
        if user:
            session['username'] = username
            return redirect(url_for('user_home'))
        return "Kullanıcı bulunamadı", 404
    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        if not users_col.find_one({"name": username}):
            users_col.insert_one({
                'name': username,
                'total_play_time': 0,
                'play_times': {},
                'ratings': [],
                'comments': []
            })
            session['username'] = username
            return redirect(url_for('user_home'))
        return "Kullanıcı zaten var", 400
    return render_template('register.html')


@app.route('/user_home')
def user_home():
    if 'username' not in session:
        return redirect(url_for('login'))
    username = session['username']
    user = users_col.find_one({"name": username})
    if not user:
        return redirect(url_for('home'))

    ratings = user.get('ratings', [])
    avg_rating = sum(r['value'] for r in ratings) / len(ratings) if ratings else None
    play_times = user.get('play_times', {})
    most_played = max(play_times, key=play_times.get) if play_times else None

    return render_template(
        'user_home.html',
        user=user,
        most_played=most_played,
        avg_rating=avg_rating
    )


@app.route('/games')
def games():
    all_games = games_col.find().sort("name", 1)
    return render_template('games.html', games=all_games)


@app.route('/game/<game_name>')
def game_detail(game_name):
    game = games_col.find_one({"name": game_name})
    if not game:
        return "Oyun bulunamadı", 404

    if game.get('ratings'):
        weighted = sum(r['play_time'] * r['value'] for r in game['ratings'])
        total_w = sum(r['play_time'] for r in game['ratings'])
        avg = weighted / total_w if total_w > 0 else None
    else:
        avg = None

    username = session.get('username')
    user = users_col.find_one({"name": username}) if username else None

    return render_template(
        'game_detail.html',
        game=game,
        average_rating=avg,
        user=user
    )


@app.route('/play/<game_name>', methods=['POST'])
def play(game_name):
    if 'username' not in session:
        return "Giriş yapmadınız", 401
    username = session['username']
    play_time = int(request.form['play_time'])
    users_col.update_one(
        {"name": username},
        {
            '$inc': {'total_play_time': play_time},
            '$set': {f'play_times.{game_name}':
                     users_col.find_one({"name": username})
                     .get('play_times', {})
                     .get(game_name, 0) + play_time}
        }
    )
    games_col.update_one({"name": game_name}, {'$inc': {'play_time': play_time}})
    return redirect(url_for('game_detail', game_name=game_name))


@app.route('/rate/<game_name>', methods=['POST'])
def rate(game_name):
    if 'username' not in session:
        return "Giriş yapmadınız", 401
    username = session['username']
    user = users_col.find_one({"name": username})
    play_time = user.get('play_times', {}).get(game_name, 0)
    if play_time < 60:
        return "Oyunu puanlamak için en az 60 dakika oynamalısınız", 403

    rating = int(request.form['rating'])
    users_col.update_one(
        {"name": username},
        {'$push': {'ratings': {'game': game_name,
                               'value': rating,
                               'play_time': play_time}}}
    )
    games_col.update_one(
        {"name": game_name},
        {'$push': {'ratings': {'user': username,
                               'value': rating,
                               'play_time': play_time}}}
    )
    return redirect(url_for('game_detail', game_name=game_name))


@app.route('/comment/<game_name>', methods=['POST'])
def comment(game_name):
    if 'username' not in session:
        return "Giriş yapmadınız", 401
    username = session['username']
    user = users_col.find_one({"name": username})
    play_time = user.get('play_times', {}).get(game_name, 0)
    if play_time < 60:
        return "Oyuna yorum yapmak için en az 60 dakika oynamalısınız", 403

    comment_text = request.form['comment']
    users_col.update_one(
        {"name": username},
        {'$push': {'comments': {'game': game_name,
                                'comment': comment_text,
                                'play_time': play_time}}}
    )
    games_col.update_one(
        {"name": game_name},
        {'$push': {'comments': {'user': username,
                                'comment': comment_text,
                                'play_time': play_time}}}
    )
    return redirect(url_for('game_detail', game_name=game_name))


@app.route('/add_game', methods=['POST'])
def add_game():
    if 'username' not in session:
        return "Giriş yapmadınız", 401
    name = request.form['name']
    genres = request.form['genres'].split(',')
    photo = request.form.get('photo', '')
    if not games_col.find_one({"name": name}):
        games_col.insert_one({
            'name': name,
            'genres': genres,
            'photo': photo,
            'play_time': 0,
            'ratings': [],
            'comments': [],
            'rating_enabled': True,
            'comment_enabled': True,
            'optional_fields': {}
        })
    return redirect(url_for('games'))


@app.route('/remove_game/<game_name>', methods=['POST'])
def remove_game(game_name):
    if 'username' not in session:
        return "Giriş yapmadınız", 401
    games_col.delete_one({"name": game_name})
    users_col.update_many({}, {
        '$pull': {
            'comments': {'game': game_name},
            'ratings': {'game': game_name}
        }
    })
    return redirect(url_for('games'))


@app.route('/remove_user/<username>', methods=['POST'])
def remove_user(username):
    if 'username' not in session:
        return "Giriş yapmadınız", 401
    user = users_col.find_one({"name": username})
    if user:
        for c in user.get('comments', []):
            games_col.update_one(
                {"name": c['game']},
                {'$pull': {'comments': {'user': username}}}
            )
        for r in user.get('ratings', []):
            games_col.update_one(
                {"name": r['game']},
                {'$pull': {'ratings': {'user': username}}}
            )
        users_col.delete_one({"name": username})
    return redirect(url_for('home'))


if __name__ == '__main__':
    # Dev server with clean shutdown on Windows
    app.run(debug=True, port=5000, use_reloader=False)
    