#Winston Venderbush
#First Flask
#9/20/16

from flask import Flask

app = Flask(__name__) #constructor

@app.route("/")
def main():
	return "Look, Dad, I made a website!"

@app.route("/1")
def idontlikeit():
	return "I don't like it very much, son..."

@app.route("/2")
def well():
	return "Well, I always liked mom more than you anyway!"

if __name__ == '__main__':
	app.run()