from flask import render_template, request, Blueprint
from myportfolio.models import Post

main = Blueprint('main', __name__)


@main.route('/')
@main.route('/home')
def home():
    return render_template('home.html')


@main.route('/work')
def work():
    page = request.args.get('page', 1, type=int)
    posts = Post.query.order_by(Post.date_posted.desc()).paginate(page=page, per_page=5)
    return render_template('work.html', title='Work', posts=posts)


@main.route('/about')
def about():
    return render_template('about.html', title='About')


@main.route('/contact')
def contact():
    return render_template('contact.html', title='Contact')