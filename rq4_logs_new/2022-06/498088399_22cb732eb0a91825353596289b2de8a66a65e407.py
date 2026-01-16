from flask import Flask, render_template
from frontend.API.companies import CompaniesApi
import json

companies_api = CompaniesApi()

app = Flask(__name__)


@app.route('/')
def index():
    title = "Biotech-h"
    return render_template('index.html', page_title=title)


@app.get('/companies')
def get_all_companies():
    all_companies = CompaniesApi.get_all()
    title = 'Список компаний'

    return render_template('companies.html', page_title=title, all_companies=all_companies)


if __name__ == "__main__":
    app.run(debug=True)