from flask import Flask, render_template, url_for, request, redirect, session

# Flask object の生成
app = Flask(__name__)
app.secret_key = 'hogehoge'

# "/"へアクセス時、"/get"へリダイレクト
@app.route("/")
def index():
    return redirect(url_for('get'))

# get処理の入力フォームを表示
@app.route("/get", methods=["GET", "POST"])
def get():
    '''
    登録ページ
    '''
    if request.method == "GET":
        return render_template('get.html')
    else:
        session['name'] = request.form.get('name')
        session['email'] = request.form.get('email')
        session['birthday'] = request.form.get('birthday')
        return redirect(url_for('confilm'))
#        return redirect(url_for('confilm'), name=name, email=email, birthday=birthday)

# getでの入力情報を確認
@app.route("/confilm", methods=["GET", "POST"])
def confilm():
    '''
    確認ページ
    '''
    if request.method == "GET":
        return render_template('confilm.html', name=session['name'], email=session['email'], birthday=session['birthday'])
    else:
#        name = request.form.get('name')
#        email = request.form.get('email')
#        birthday = request.form.get('birthday')
#        return render_template('confilm.html', name=name, email=email, birthday=birthday)
#        return render_template('confilm.html')
        return redirect(url_for('complete'))


@app.route("/complete")
def complete():
    '''
    完了ページ
    '''
    return render_template('complete.html')


# debugありで起動
if __name__ == "__main__":
    app.run(debug=True)