from flask import Flask, escape, request, render_template

app = Flask(__name__)

@app.route('/')
def hello():
    name = request.args.get("name", "World")
    return f'Hello, {escape(name)}!'

@app.route('/ping')
def ping():
    return render_template('ping.html')
@app.route('/pong',methods=['GET','POST'])
def pong():
    #request.args => get방식으로 데이터가 들어올 때.
    #request.form => post방식으로 데이터가 들어올 때.
    #print(request.form.get('keyword'))
    keyword=request.form.get('keyword')
    return render_template('pong.html',keyword=keyword)

if __name__ == '__main__':
    app.run(debug=True)