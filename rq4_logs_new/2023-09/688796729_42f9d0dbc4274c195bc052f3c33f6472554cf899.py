from flask import Flask, render_template, url_for
app = Flask(__name__)
posts = [
        {
        'author':'Krushang',
        'Title':'AutoBiography',
        'content':'FirstPostContent',
        'date':'21 September'
        },
        {
        'author':'Palky',
        'Title':'Nahane Jaa',
        'content':'Variyali',
        'date':'28 September'
        }
    ]
@app.route('/')
@app.route('/home')
def home():
    return render_template('home.html', posts=posts)
@app.route('/about')
def about():
    return render_template('about.html', posts=posts)
if __name__ == '__main__':
    app.run(debug=True)