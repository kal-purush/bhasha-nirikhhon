from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/calculate', methods=['POST'])
def calculate():
    cashflows = request.form.getlist('cashflow')
    rate_of_return = float(request.form['rate_of_return'])

    npv = 0
    for i, cashflow in enumerate(cashflows):
        npv += float(cashflow) / (1 + rate_of_return) ** (i + 1)

    return render_template('result.html', npv=npv)

if __name__ == '__main__':
    app.run(debug=True)