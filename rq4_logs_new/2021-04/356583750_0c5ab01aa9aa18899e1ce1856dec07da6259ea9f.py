from flask import Flask,render_template,request
from lyrics_classify_model import predict,pipeline


app = Flask(__name__)

@app.route('/')
def main_page():
    return render_template('main_page.html')


@app.route('/lyrics_artist_prediction')
def get_predictions():

    lyrics = request.args
    artist,prob=predict(pipeline,lyrics)
    prob = round(prob,2)
    
    return render_template('prediction_results.html',artist=artist,prob=prob,lyrics=lyrics)
    

if __name__ == "__main__":
    app.run(debug=True, port=5000)
    