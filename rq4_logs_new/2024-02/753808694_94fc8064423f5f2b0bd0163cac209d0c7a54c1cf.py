import json

with open('filmes.json', 'r') as file:
    filmesDB = json.load(file)

generosDB = {}
atoresDB = {}
#filmes_listDB = []
g_count = 0
a_count = 0

for filme in filmesDB:
    generos = filme.get('genres', [])
    atores = filme.get('cast', [])

    """filmes_listDB.append({
        "_id": {
            "$oid": filme['_id']['$oid']
            },
        "nome": filme['title']
    })"""

    if not generos:
        generos = ['None']

    for genero in generos:
        if genero not in generosDB:
            generosDB[genero] = {
                "id": f"g{g_count}",
                "nome": genero,
                "filmes": [filme['title']]
            }
            g_count += 1
        else:
            generosDB[genero]["filmes"].append(filme['title'])

    if atores:
        for ator in atores:
            if ator not in atoresDB:
                atoresDB[ator] = {
                    "id": f"a{a_count}",
                    "nome": ator,
                    "filmes": [filme['title']]
                }
                a_count += 1
            else:
                atoresDB[ator]["filmes"].append(filme['title'])

newDB = {
    "filmes": filmesDB,
    #"filmeslist": filmes_listDB,
    "generos": list(generosDB.values()),
    "atores": list(atoresDB.values())
}

with open('newDB.json', 'w') as file:
    json.dump(newDB, file, indent=2)