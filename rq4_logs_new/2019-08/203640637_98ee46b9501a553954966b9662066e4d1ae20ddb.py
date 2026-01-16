# Imports
import graphene

from flask import Flask
from flask_graphql import GraphQLView

from mongoengine import connect
from schemas.schemas import Query

# app initialization
app = Flask(__name__)
connect('loans', host="mongodb://localhost:27017", alias="default")
app.debug = True

# Configs
# TO-DO

# Modules
# TO-DO

# Routes
@app.route('/')
def index():
    return '<p>Hello, Autarquias!</p><p>Access /graphql to the GraphQL interface.</p>'

schema = graphene.Schema(query=Query)

app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view(
        'graphql',
        schema=schema,
        graphiql=True # for having the GraphiQL interface
    )
)

if __name__ == '__main__':
     app.run()