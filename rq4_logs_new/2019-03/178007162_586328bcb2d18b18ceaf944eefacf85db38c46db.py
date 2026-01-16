#coding:utf-8
"""
info:   
author: NetFj@sina.com
file:   main.py
time:   2019/3/25.20:26
"""

from flask import Flask, render_template, flash,get_flashed_messages,request,redirect,url_for
from flask_sqlalchemy import SQLAlchemy
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired

app = Flask(__name__)
app.config['SECRET_KEY']='123456'
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:root@localhost/flask_books'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# 构建模型
class Author(db.Model):
    __tablename__ = 'authors'
    id      = db.Column(db.Integer,primary_key=True)
    name    = db.Column(db.String(16),unique=True)

    # 关联
    books = db.relationship('Book',backref='author')

    def __repr__(self):
        return 'Author: {}{}'.format(self.id,self.name)

class Book(db.Model):
    __tablename__ = 'books'
    id          = db.Column(db.Integer,primary_key=True)
    name        = db.Column(db.String(16),unique=True)
    author_id   = db.Column(db.Integer,db.ForeignKey('authors.id'))
    def __repr__(self):
        return 'Book:{}|{}'.format(self.id,self.name)

# 建表
db.drop_all()
db.create_all()

# 添加数据
au1 = Author(id=1,name='张三')
au2 = Author(id=2,name='李四')
au3 = Author(id=3,name='王五爷')
bk1 = Book(name='回忆录',author_id=au1.id)
bk2 = Book(name='如何成为一个英雄',author_id=au1.id)
bk3 = Book(name='一个伟大的时刻',author_id=au2.id)
bk4 = Book(name='我的哲学思想',author_id=au3.id)
bk5 = Book(name='一百年前',author_id=au3.id)
db.session.add_all([au1,au2,au3,bk1,bk2,bk3,bk4,bk5])
db.session.commit()

# 自定义表单类
class AuthorForm(FlaskForm):
    author  = StringField('作者',validators=[DataRequired()])
    book    = StringField('书籍',validators=[DataRequired()])
    submit  = SubmitField('提交')

# 主页面
@app.route('/',methods=['GET','POST'])
def index():
    #创建自定义表单类
    author_form = AuthorForm()

    #验证
    if author_form.validate_on_submit():
        author_name = author_form.author.data
        book_name   = author_form.book.data

        # 作者是否存在
        author = Author.query.filter_by(name=author_name).first()
        if author:  # 作者存在
            # 书籍是否存在
            book = Book.query.filter_by(name=book_name).first()
            if book:    # 书籍存在
                flash('已经存在重名书籍')
            else:
                try:
                    new_book = Book(name=book_name,author_id=author.id)
                    db.session.add(new_book)
                    db.session.commit()
                    flash('添加书籍成功')
                except Exception as e:
                    print('Error:',e)
                    flash('添加书籍失败')
                    db.session.rollback()   #回滚
        else:
            # 加作者、加新书
            try:
                new_author = Author(name = author_name)
                db.session.add(new_author)
                db.session.commit()
                new_book = Book(name=book_name,author_id=new_author.id)
                db.session.add(new_book)
                db.session.commit()
                flash('添加作者书籍成功！')
            except Exception as e:
                print('Error:',e)
                flash('添加作者书籍失败')
                db.session.rollback()
    else:
        if request.method == 'POST':
            flash('参数不全')

    # 查询作者信息
    authors = Author.query.all()

    return render_template('books.html',authors=authors,form=author_form)

# 删除作者
@app.route('/delete_author/<author_id>')
def delete_author(author_id):
    author = Author.query.get(author_id)
    if author:
        try:
            Book.query.filter_by(author_id=author.id).delete()
            db.session.delete(author)
            db.session.commit()
            msg = '作者删除成功:{}/{}'.format(author.id,author.name)
            flash(msg)
        except Exception as e:
            db.session.rollback()
            msg = '作者删除不成功:{}/{}'.format(author.id, author.name)
            flash(msg)
    else:
        flash('作者找不到！')
    return redirect(url_for('index'))

# 删除书籍
@app.route('/delete_book/<book_id>')
def delete_book(book_id):
    book = Book.query.get(book_id)
    if book:
        try:
            db.session.delete(book)
            db.session.commit()
            msg = '书籍删除了:{}/{}'.format(book.id,book.name)
            flash(msg)
        except Exception as e:
            print('Error:', e)
            db.session.rollback()
    else:
        flash('书籍找不到！')

    # 重定向路径
    return redirect(url_for('index'))

if __name__ == "__main__":
    app.run(debug=True)
