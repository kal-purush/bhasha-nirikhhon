import bottle, sqlite3 as sql, bcrypt, secrets

MAX_POSTS = 25

db = sql.connect("data.db")

sessions = {}


def setup_db():
    c = db.cursor()
    is_setup = c.execute("SELECT name FROM sqlite_master WHERE name='users';")

    if is_setup.fetchone() is None:
        c.execute("CREATE TABLE users(userId int, username text, passwd blob)")
        c.execute("CREATE TABLE posts(postId int, title text, content longtext)")
        db.commit()


def create_session(user_id):
    key = secrets.token_hex(16)
    sessions[key] = user_id
    return key


def get_user(uid=None, username=None):
    c = db.cursor()
    if uid != None:
        q = c.execute("SELECT * FROM users WHERE userId=?", (uid,))
        return q.fetchone()
    elif username != None:
        q = c.execute("SELECT * FROM users WHERE username=?", (username,))
        return q.fetchone()


@bottle.get('/styles.css')
def stylesheets():
    return bottle.static_file("styles.css", root='static/')


@bottle.get('/')
def index_redir():
    bottle.redirect('/mb?status=home')


@bottle.get('/mb')
def main_page():
    status = bottle.request.query.status
    page = '<link rel="stylesheet" href="styles.css">'
    login_page = ""
    with open("static/login.html", "r") as f:
        login_page += f.read().strip()

    if status=="home":
        page += login_page

    elif status=="userexists":
        page += '<p id="error">Error: that username is already in use</p><br/>'
        page += login_page

    elif status=="noexist":
        page += '<p id="error">Error: no such user exists</p><br/>'
        page += login_page

    elif status=="badpass":
        page += '<p id="error">Error: incorrect password</p><br/>'
        page += login_page

    return page

@bottle.post('/signup')
def signup():
    username = bottle.request.forms.get("username")
    passwd = bottle.request.forms.get("passwd")
    usr = get_user(username=username)
    if usr is not None:
        bottle.redirect("/mb?status=userexists")
    else:
        salt = bcrypt.gensalt()
        passwd = bcrypt.hashpw(passwd.encode('UTF-8'), salt=salt)
        c = db.cursor()
        c.execute("SELECT * FROM users WHERE userId = (SELECT MAX(userId) FROM users)")
        lastId = c.fetchone()
        if lastId is None:
            lastId = (0,)
        lastId = int(lastId[0])
        uid = lastId+1
        c.execute("INSERT INTO users VALUES(?, ?, ?)", (uid, username, passwd))
        db.commit()
        ses = create_session(uid)
        bottle.response.set_cookie(name="session", value=ses)
        bottle.redirect("/mb?status=main")

@bottle.post('/login')
def login():
    username = bottle.request.forms.get("username")
    passwd = bottle.request.forms.get("passwd")
    usr = get_user(username=username)
    if usr is None:
        bottle.redirect("/mb?status=noexist")
    else:
        stored_passwd = usr[2]
        if bcrypt.checkpw(passwd.encode(), stored_passwd):
            ses = create_session(usr[0])
            bottle.response.set_cookie(name="session", value=ses)
            bottle.redirect("/mb?status=main")
        else:
            bottle.redirect("mb?status=badpass")

if __name__ == "__main__":
    setup_db()
    bottle.run(host='0.0.0.0', port=8080, debug=True)