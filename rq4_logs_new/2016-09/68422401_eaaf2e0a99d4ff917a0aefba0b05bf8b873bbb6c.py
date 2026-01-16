from firebase import firebase
from flask import Flask
from .forms import FirePut

app = Flask(__name__)
firebase = firebase.FirebaseApplication(‘https://hackcmu2016-a3c18.firebaseio.com/’, None)
count = 0
@app.route(‘/api/put’, methods=[‘GET’, ‘POST’])
def fireput():
  form = FirePut()
  If form.validate_on_submit():
    global count
    count += 1
    putData = {‘Title’ : form.title.data, ‘Year’ : form.year.data, ‘Rating’ : form.rating.data}
    firebase.put(‘/films’, ‘film’ + str(count), putData)
    return render_template(‘api-put-result.html’, form=form, putData=putData)
  return render_template(‘My-Form.html’)