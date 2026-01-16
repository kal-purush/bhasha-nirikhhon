from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def main():
	return render_template('index.html')

@app.route('/Top3.html')
def Top3():
    return render_template('Top3.html')

@app.route('/Contact.html')
def contact():
    return render_template('Contact.html')

@app.route('/Contact/Post.html', methods=["post"])
def contactpost():
    name = request.form['name']
    email = request.form['email']
    phonenumber = request.form['phonenumber']
    description = request.form['description']
    return (name+email+phonenumber+description)

if __name__ == '__main__':
	app.config['DEBUG'] = True
	app.run(host='0.0.0.0')