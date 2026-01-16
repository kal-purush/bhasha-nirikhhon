import base64
import configparser
import os
import sqlite3
import warnings

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from flask_login import login_user, logout_user, current_user, LoginManager, UserMixin
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Table, create_engine
from werkzeug.security import generate_password_hash, check_password_hash

external_stylesheets = [
    dbc.themes.SLATE,  # Bootstrap theme
    'https://www.kaggle.com/sripaadarinivasan/kickstarter-campaigns-dataset',  # kickstarter data
]

meta_tags = [
    {'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets, meta_tags=meta_tags)
server = app.server
app.config.suppress_callback_exceptions = True  # see https://dash.plot.ly/urls

server.config.update(
    SECRET_KEY=os.urandom(12),
    SQLALCHEMY_DATABASE_URI='sqlite:///data.sqlite',
    SQLALCHEMY_TRACK_MODIFICATIONS=False
)
app.title = 'Kickstarter Funding Predicted'  # appears in browser title bar

warnings.filterwarnings("ignore")
sqlite3.connect('data.sqlite')
engine = create_engine('sqlite:///data.sqlite')
db = SQLAlchemy()
config = configparser.ConfigParser()


class Users(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(15), unique=True, nullable=False)
    email = db.Column(db.String(50), unique=True)
    password = db.Column(db.String(80))


Users_tbl = Table('users', Users.metadata)


def create_users_table():
    Users.metadata.create_all(engine)


create_users_table()


db.init_app(server)

navbar = dbc.NavbarSimple(
    brand='Kickstarter Funding Predictor',
    brand_href='/',
    children=[
        dbc.NavItem(dcc.Link('Predictions', href='/predictions', className='nav-link')),
    ],
    sticky='top',
    color='primary',
    light=False,
    dark=True
)

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    navbar,
    dbc.Container(id='page-content', className='mt-4'),
    html.Hr(),
])

login_manager = LoginManager()
login_manager.init_app(server)
login_manager.login_view = '/login'


class Users(UserMixin, Users):
    pass


column1 = dbc.Col(
    [
        dcc.Markdown(
            """
            ## This app will aid potential Kickstarter campaigns in predicting whether or not their campaign will be funded.
            """
        ),
        dcc.Link(dbc.Button('Predict my Kickstarter!!!', color='success'), href='/create')
    ],
    md=4,
)

# image_filename = '/Users/nikihodges/PycharmProjects/pythonProject8/venv/assets/kickstarter.png'
# encoded_image = base64.b64encode(open(image_filename, 'rb').read())

# column2 = dbc.Col(
#     [html.Div([html.Img
#                (src='data:image/png;base64,{}'.format(encoded_image.decode()
#                                                       ))])])

index = dbc.Row([column1])

create = html.Div([html.H1('Create User Account'),
                   dcc.Location(id='create_user', refresh=True),
                   dcc.Input(id="username",
                             type="text",
                             placeholder="user name",
                             maxLength=15),
                   dcc.Input(id="password",
                             type="password",
                             placeholder="password"),
                   dcc.Input(id="email",
                             type="email",
                             placeholder="email",
                             maxLength=50),
                   html.Button('Create User', id='submit-val', n_clicks=0),
                   html.Div(id='container-button-basic')])

login = html.Div([dcc.Location(id='url_login', refresh=True),
                  html.H2('''Please log in to continue:''', id='h1'),
                  dcc.Input(placeholder='Enter your username',
                            type='text',
                            id='uname-box'),
                  dcc.Input(placeholder='Enter your password',
                            type='password',
                            id='pwd-box'),
                  html.Button(children='Login',
                              n_clicks=0,
                              type='submit',
                              id='login-button'),
                  html.Div(children='', id='output-state')])

success = html.Div([dcc.Location(id='url_login_success', refresh=True),
                    html.Div([html.H2('Login successful.'),
                              html.Br(),
                              html.P('Go to Predictor'),
                              dcc.Link(dbc.Button('Predict', color='danger'),
                                       href='/predictions')]),
                    html.Div([html.Br(),
                              html.Button(id='back-button',
                                          children='Go back', n_clicks=0)])])

failed = html.Div([dcc.Location(id='url_login_df', refresh=True),
                   html.Div([html.H2('Log-in Failed.  Please try again.'),
                             html.Br(),
                             html.Div([login]),
                             html.Br(),
                             html.Button(id='back-button', children='Go back', n_clicks=0)]
                            )])

logout = html.Div([dcc.Location(id='logout', refresh=True),
                   html.Br(),
                   html.Div(html.H2('You have been logged out - Please login')),
                   html.Br(),
                   html.Div([login]),
                   html.Button(id='back-button', children='Go back', n_clicks=0)])


@login_manager.user_loader
def load_user(user_id):
    return Users.query.get(int(user_id))


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/':
        return index
    elif pathname == '/create':
        return create
    elif pathname == '/login':
        return login
    elif pathname == '/success':
        if current_user.is_authenticated:
            return success
        else:
            return failed
    elif pathname == '/predictions':
        return predictions
    elif pathname == '/logout':
        if current_user.is_authenticated:
            logout_user()
            return logout
        else:
            return logout
    else:
        return '404'


@app.callback(
    [Output('container-button-basic', "children")],
    [Input('submit-val', 'n_clicks')],
    [State('username', 'value'),
     State('password', 'value'), State('email', 'value')])
def insert_users(n_clicks, un, pw, em):
    if un is not None and pw is not None and em is not None and n_clicks > 0:
        hashed_password = generate_password_hash(pw, method='pbkdf2:sha256', salt_length=16)
        ins = Users_tbl.insert().values(username=un, password=hashed_password, email=em, )
        conn = engine.connect()
        conn.execute(ins)
        conn.close()
        return [login]
    else:
        return [html.Div([html.H2('Already have a user account?'),
                          dcc.Link('Click here to Log In', href='/login')])]


@app.callback(
    Output('url_login', 'pathname'),
    [Input('login-button', 'n_clicks')],
    [State('uname-box', 'value'), State('pwd-box', 'value')])
def successful(n_clicks, input1, input2):
    user = Users.query.filter_by(username=input1).first()
    if user:
        if check_password_hash(user.password, input2):
            login_user(user)
            return '/success'
        else:
            pass
    else:
        pass


@app.callback(
    Output('output-state', 'children'),
    [Input('login-button', 'n_clicks')],
    [State('uname-box', 'value'), State('pwd-box', 'value')])
def update_output(n_clicks, input1, input2):
    if n_clicks > 0:
        user = Users.query.filter_by(username=input1).first()
        if user:
            if check_password_hash(user.password, input2):
                return ''
            else:
                return 'Incorrect username or password'
        else:
            return 'Incorrect username or password'
    else:
        return ''


if __name__ == "__main__":
    app.run_server(debug=True, threaded=False, processes=2)