from flask import Blueprint, render_template

from frontend.api.companies import CompaniesApi
from frontend.config import config

companies_api = CompaniesApi(config.backend.url)


routes = Blueprint('companies', __name__)


@routes.get('/companies')
def get_all_companies():
    companies = companies_api.get_all()

    return render_template('companies.html', page_title='Список компаний', companies=companies)