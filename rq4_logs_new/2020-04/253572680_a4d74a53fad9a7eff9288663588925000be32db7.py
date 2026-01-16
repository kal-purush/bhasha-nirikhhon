from flask import Flask, render_template, url_for
app = Flask(__name__)

posts = [
    {
        'workTitle': 'Work1',
        'skill': 'work1 work1'
    },
    {
        'workTitle': 'Work2',
        'skill': 'work2 work2'
    },
    {
        'workTitle': 'Work3',
        'skill': 'work3 work3'
    }
]
 
@app.route('/')
@app.route('/home')
def home():
    return render_template('home.html')

@app.route('/work')
def work():
    return render_template('work.html', title='Work', posts=posts)

@app.route('/about')
def about():
    return render_template('about.html', title='About')

@app.route('/contact')
def contact():
    return render_template('contact.html', title='Contact')

def main():
    app.debug = True
    app.run()
 
if __name__ == '__main__':
    main()