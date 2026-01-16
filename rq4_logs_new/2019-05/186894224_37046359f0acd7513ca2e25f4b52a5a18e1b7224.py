from flask import Flask, request, redirect, render_template, flash
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config['DEBUG'] = True
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://build-a-blog:abc123@localhost:3306/build-a-blog'
app.config['SQLALCHEMY_ECHO'] = True
db = SQLAlchemy(app)
app.secret_key = 'abc123'

class Blog(db.Model):

    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(120))
    body = db.Column(db.String(10000))

    def __init__(self, title, body):
        self.title = title
        self.body = body


@app.route('/', methods=['POST', 'GET'])
def index():
    posts = Blog.query.all()   
    return render_template('blog.html', posts=posts)


@app.route('/newpost', methods=['POST', 'GET'])
def newpost():
    if request.method == 'GET':
        return render_template('newpost.html')
    if request.method == 'POST':
        title = request.form['title']
        body = request.form['body']
        if title == '':
            return render_template('newpost.html', message="You Must Enter A Title", title=title, body=body)
        if body == '':
            return render_template('newpost.html', message1="You Must Enter A Post Body", title=title, body=body)
        else:
            new_post = Blog(title,body)
            db.session.add(new_post)
            db.session.commit()
            ids = new_post.id
            single_post = Blog.query.get(ids)
            return render_template('blog_post.html',single_post=single_post)


@app.route('/blog', methods=['POST', 'GET'])
def blog():
    post_id = request.args.get('id')
    print(post_id)
    if post_id == None:
        return redirect('/')
    else:
        single_post = Blog.query.get(post_id)
        return render_template('blog_post.html',single_post=single_post)



if __name__ == '__main__':
    app.run()