import json
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from datetime import datetime

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///C:\\WINDOWS\\TEMP\\test.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
app.config['FLASK_ADMIN_SWATCH'] = 'cerulean'
app.config['SECRET_KEY'] = '123'
db = SQLAlchemy(app)
admin = Admin(app, name='test', template_mode='bootstrap3')


@app.route('/user/post/')
def hello():
    user = User.query.first()
    post_dict = []
    print(user.posts)
    for post in user.posts:
        d = {
            '標題': post.title,
            '內文': post.body,
            '作者': post.user.username
        }
        post_dict.append(d)
    
    return json.dumps(post_dict)

@app.route('/user/createPost/')
def create():
    
    title = '標題2'
    body = '內文2'
    user = User.query.first()
    category = Category.query.first()
    
    post = Post(title, body, category, user.id)
    db.session.add(post)
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print(e)

    return f'標題: {post.title}, 內文: {post.body}, 作者: {user}'



class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True)
    email = db.Column(db.String(120), unique=True)
    

    def __init__(self, username, email):
        self.username = username
        self.email = email

    def __repr__(self):
        return self.username


class Post(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(80))
    body = db.Column(db.Text)
    pub_date = db.Column(db.DateTime)

    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    user = db.relationship('User',
        backref=db.backref('posts', lazy='joined'))
    category_id = db.Column(db.Integer, db.ForeignKey('category.id'))
    category = db.relationship('Category',
        backref=db.backref('posts', lazy='dynamic'))

    # def create(self, title, body, category, user_id=None, pub_date=None):
    #     self.title = title
    #     self.body = body
    #     if pub_date is None:
    #         pub_date = datetime.utcnow()
    #     self.pub_date = pub_date
    #     self.category = category
    #     self.user_id = user_id

    def __repr__(self):
        return '<Post %r>' % self.title

class Category(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<Category %r>' % self.name

admin.add_view(ModelView(User, db.session))
admin.add_view(ModelView(Post, db.session))
admin.add_view(ModelView(Category, db.session))


if __name__ == '__main__':
    app.run(debug=True)


@app.shell_context_processor
def make_shell_context():
    return dict(
        app=app,
        db=db,
        User=User,
        Post=Post,
        Category=Category
    )