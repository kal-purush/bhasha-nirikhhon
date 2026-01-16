import graphene

class Query(graphene.ObjectType):
    # //graphql attribute
    hello = graphene.String(name=graphene.String(default_value="stranger"))
    bolo = graphene.String(bolo=graphene.String(default_value="bolo stranger"))

    def resolve_hello(self, info, name):
        return 'Hello ' + name
    def resolve_bolo(self,info,bolo):
        return bolo

# schema describes the data model and provides the graphql server with the
# resolve method
schema = graphene.Schema(query=Query)
# schema1 =graphene.Schema()
"""
quering the schema using schema.execute method
When you call the hello and bolo attributes on Query using schema object,
it takes the response from hello and returns it
"""
result = schema.execute('{bolo}')
result2 = schema.execute('{hello}')

print(result2.data,result.data)