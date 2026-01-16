from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def input() :
    return render_template('input.html')

@app.route('/result',method=['POST','GET'])
def result() :
    if(request.method=='POST'):
        result = dict()
        result['Name'] = request.form.get('name')
        result['StuedntNumber'] = request.form.get('StudentNumber')
        result['language'] = request.form.getlist('languages')
        result['languages'] = ','.join(result['languages'])
        return render_template('result.html',result=result)
    
app.run(debug=1)