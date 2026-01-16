from flask_login import UserMixin
from app import login


class User(UserMixin):
    id
    username = ''
    password = ''
    role = ''


@login.user_loader
def load_user(id):
    user = User()
    user.username = 'admin'
    user.password = 'admin'
    user.role = 'admin'
    user.id = 1
    return user