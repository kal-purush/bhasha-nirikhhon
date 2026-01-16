from flask import Flask, render_template, request, jsonify
import requests
from SPARQLWrapper import SPARQLWrapper, JSON

app = Flask(__name__)

graphdb_endpoint = "http://localhost:7200/repositories/DRE"


@app.route('/')
def index():
    return render_template('frontend/pagina-inicial/index.html')

@app.route('/search')
def search():
    search_term = request.args.get('term', '')

    # Perform the search
    documents = search_documents(search_term)
    authors = search_authors(search_term)

    # Return the search results as JSON
    return jsonify({'documents': documents, 'authors': authors})

def search_documents(search_term):
    query = f"""
    PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT ?doc (STRAFTER(STR(?doc), "Diario_Replublica/") AS ?doc_id)
    WHERE {{
      ?doc rdf:type :Document .
      FILTER(contains(str(?doc), "{search_term}"))
    }} limit 10
    """
    
    sparql = SPARQLWrapper(graphdb_endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    
    documents = [result['doc_id']['value'] for result in results['results']['bindings']]
    
    return documents

def search_authors(search_term):
    query = f"""
    PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

    SELECT ?author ?name
    WHERE {{
      ?author rdf:type :Emitter .
      ?author :emitter_name ?name .
      FILTER(contains(?name, "{search_term}"))
    }} limit 10
    """

    sparql = SPARQLWrapper(graphdb_endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    
    authors = [result['name']['value'] for result in results['results']['bindings']]

    return authors

@app.route('/documentos')
def documentos():
    return render_template('frontend/documentos/index.html')

@app.route('/autores')
def autores():
    return render_template('frontend/autores/index.html')


@app.route('/documentos/all_pag=<string:np>')
def todos_documentos(np):
    query = """
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT  (count(distinct ?doc) as ?count)
WHERE {
  ?doc rdf:type :Document .
}
    """
    resposta = requests.get(graphdb_endpoint, 
                                    params={'query': query},
                                    headers={'Accept': 'application/sparql-results+json'})
    nDocs=0
    pag = 0
    r = 0
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        nDocs = int(dados[0]["count"]["value"])
        pag = nDocs//150
        r = nDocs%150
        if r > 0:
            pag += 1
    offset = (int(np)-1)*150
    query = f"""
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?doc
WHERE {{
  ?doc rdf:type :Document .
}}
ORDER BY ?doc
LIMIT 150
OFFSET {offset}
"""
    
    resposta = requests.get(graphdb_endpoint, 
                                    params={'query': query},
                                    headers={'Accept': 'application/sparql-results+json'})
        
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        list_doc = []
        for dado in dados:
            list_doc.append(dado["doc"]["value"].split("/")[-1])
        return render_template('frontend/lista-documentos/index.html', np=int(np),pag=pag, dados=list_doc)
    
@app.route('/documentos/by_author/<string:id>_pag=<string:np>')
def documentos_por_autor(id,np):
    query = f""" 
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT (count(distinct ?doc) as ?count)
WHERE {{
  ?doc rdf:type :Document.
  ?doc :hasEmitter :{id}.
      
      }}
"""
    resposta = requests.get(graphdb_endpoint, 
                                    params={'query': query},
                                    headers={'Accept': 'application/sparql-results+json'})
    nDocs=0
    pag = 0
    r = 0
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        nDocs = int(dados[0]["count"]["value"])
        pag = nDocs//150
        r = nDocs%150
        if r > 0:
            pag += 1
    offset = (int(np)-1)*150

    query = f"""
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?doc
WHERE {{
  ?doc rdf:type :Document.
  ?doc :hasEmitter :{id}.   
      }}
      ORDER BY ?doc
    LIMIT 150
    OFFSET {offset}
"""
    resposta = requests.get(graphdb_endpoint, 
                                    params={'query': query},
                                    headers={'Accept': 'application/sparql-results+json'})
        
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        list_doc = []
        for dado in dados:
            list_doc.append(dado["doc"]["value"].split("/")[-1])
        return render_template('frontend/documentos-por-autor/index.html', autor = id,np=int(np),pag=pag, dados=list_doc)

@app.route('/documentos/by_day')
def escolher_dia():
    return render_template('frontend/escolher-dia/index.html')

@app.route('/documentos/by_day/<string:day>_pag=<string:np>')
def documentos_por_dia(day,np):
    query = f""" 
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT  (count(distinct ?doc) as ?count)
WHERE {{
  ?doc rdf:type :Document .
  ?doc :date "{day}"
}}
    """
    resposta = requests.get(graphdb_endpoint, 
                                    params={'query': query},
                                    headers={'Accept': 'application/sparql-results+json'})
    ndocs=0
    pag = 0
    r = 0
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        ndocs = int(dados[0]["count"]["value"])
        pag = ndocs//150
        r = ndocs%150
        if r > 0:
            pag += 1
    offset = (int(np)-1)*150
    
    query = f"""
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?doc
WHERE {{
  ?doc rdf:type :Document .
  ?doc :date "{day}"
}}
ORDER BY ?doc
LIMIT 150
OFFSET {offset}
"""
    resposta = requests.get(graphdb_endpoint, 
                                    params={'query': query},
                                    headers={'Accept': 'application/sparql-results+json'})
        
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        list_doc = []
        for dado in dados:
            list_doc.append(dado["doc"]["value"].split("/")[-1])
        return render_template('frontend/documentos-por-dia/index.html', np=int(np),pag=pag, dados=list_doc,day = day)

@app.route('/documentos/by_type_pag=<string:np>')
def documentos_tipo(np):
    query = """
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT (count(distinct ?type) as ?count)
WHERE {{
  ?doc rdf:type :Document.
  ?doc :docType ?type.   
      }} order by ?type
    """
    resposta = requests.get(graphdb_endpoint, 
                                    params={'query': query},
                                    headers={'Accept': 'application/sparql-results+json'})
    ndocs=0
    pag = 0
    r = 0
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        ndocs = int(dados[0]["count"]["value"])
        pag = ndocs//150
        r = ndocs%150
        if r > 0:
            pag += 1
    offset = (int(np)-1)*150
    
    query = f"""
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT Distinct ?type
WHERE {{
  ?doc rdf:type :Document.
  ?doc :docType ?type.   
      }} order by ?type
    LIMIT 150
    OFFSET {offset}
    
"""
    resposta = requests.get(graphdb_endpoint, 
                                    params={'query': query},
                                    headers={'Accept': 'application/sparql-results+json'})
        
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        list_types = []
        for dado in dados:
            list_types.append(dado["type"]["value"])
        return render_template('frontend/lista-tipos-documentos/index.html', np=int(np),pag=pag, dados=list_types)
       


@app.route('/documentos/by_type/<string:type>_pag=<string:np>')
def documentos_por_tipo(type,np):
    query = f"""
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT (count(distinct ?doc) as ?count)
WHERE {{
  ?doc rdf:type :Document .
  ?doc :docType "AVISO"
}}
ORDER BY ?doc
    """
    resposta = requests.get(graphdb_endpoint, 
                                    params={'query': query},
                                    headers={'Accept': 'application/sparql-results+json'})
    ndocs=0
    pag = 0
    r = 0
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        ndocs = int(dados[0]["count"]["value"])
        pag = ndocs//150
        r = ndocs%150
        if r > 0:
            pag += 1
    offset = (int(np)-1)*150
    
    query = f"""
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?doc
WHERE {{
  ?doc rdf:type :Document .
  ?doc :docType "{type}"
}}
ORDER BY ?doc
LIMIT 150
OFFSET {offset}
"""
    resposta = requests.get(graphdb_endpoint, 
                                    params={'query': query},
                                    headers={'Accept': 'application/sparql-results+json'})
        
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        list_docs = []
        for dado in dados:
            list_docs.append(dado["doc"]["value"].split("/")[-1])
        return render_template('frontend/documentos-por-tipo/index.html', np=int(np), pag=pag, tipo = type, dados=list_docs)
      


@app.route('/documento/<string:id>')
def documento_info(id):
    query = f"""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
SELECT ?emitter ?date ?type ?notes ?number  ?series ?source
WHERE {{
  :{id} rdf:type :Document ;
      :hasEmitter ?emitter;
      :docType ?type; 
      :number ?number.
  optional{{:{id} :date ?date;
    :notes ?notes;
	:series ?series;
    :source ?source.}}
}} 
"""
    resposta = requests.get(graphdb_endpoint, 
                          params = {"query": query}, 
                          headers = {'Accept': 'application/sparql-results+json'})
    if resposta.status_code == 200 :
        dados = resposta.json()['results']['bindings']
        dic = {}
        dic['id'] = id
        dic["emitter"] = []
        for dado in dados:
            dic["emitter"].append(dado["emitter"]["value"].split("/")[-1])
            if "date" in dado:
                dic["date"] = dado["date"]["value"]
            else:
                dic["date"] = ""
            dic["type"] = dado["type"]["value"]
            if "notes" in dado:
                dic["notes"] = dado["notes"]["value"]
            else: 
                dic["notes"] = ""
            dic["name"] = dic["type"] + " " + dado["number"]["value"]
            if "series" in dado:
                dic["series"] = dado["series"]["value"]
            else:
                dic["series"] = ""
            if "source" in dado:
                dic["source"] = dado["source"]["value"]
            else:
                dic["source"] = ""
        return render_template('frontend/informacao-documento/index.html',data = dic)


@app.route('/documentos/add', methods=['GET', 'POST'])
def create_document():
    if request.method == 'POST':
        document_id = request.form['documentID']
        document_type = request.form['documentType']
        emitter = request.form['emitter']
        document_number = request.form['documentNumber']
        document_number_dr = request.form.get('documentNumberdr', "")
        document_date = request.form.get('documentDate', "")
        document_series = request.form.get('documentSeries', "")
        document_source = request.form.get('documentSource', "")
        document_notes = request.form.get('documentNotes', "")  

        
        query = f"""
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

INSERT DATA {{
  :{document_id} rdf:type :Document ;
			   :hasEmitter :{emitter};
               :docType "{document_type}" ;  
               :number "{document_number}"
"""
        if document_number_dr != "":
            query += f';\n:numberDR "{document_number_dr}" '
        if document_date != "":
            query += f';\n:date "{document_date}" '
        if document_series != "":
            query += f';\n:series {document_series} '
        if document_source != "":
            query += f';\n:source "{document_source}" '
        if document_notes != "":
            query += f';\n:notes "{document_notes}" '
        query += ".\n }"
    
        try:
            sparql = SPARQLWrapper(graphdb_endpoint + "/statements")
            sparql.setMethod('POST')
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            sparql.query().convert()
            return jsonify({"success": True, "message": "Documento adicionado com sucesso!"})
        except Exception as e:
            return jsonify({"success": False, "message": " Documento não foi adicionado!"})
    return render_template('frontend/inserir-documento/index.html')

@app.route('/autores/all_pag=<string:np>')
def todos_autores(np):
    query = """
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT  (count(distinct ?emi) as ?count)
WHERE {
  ?emi rdf:type :Emitter .
}    
"""
    resposta = requests.get(graphdb_endpoint, 
                                    params={'query': query},
                                    headers={'Accept': 'application/sparql-results+json'})
    nEmiters=0
    pag = 0
    r = 0
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        nEmiters = int(dados[0]["count"]["value"])
        pag = nEmiters//150
        r = nEmiters%150
        if r > 0:
            pag += 1
    offset = (int(np)-1)*150
    query = f"""
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?emit_name
WHERE {{
  ?emit rdf:type :Emitter .
  ?emit :emitter_name ?emit_name.
  
}}
ORDER BY ?emit_name
LIMIT 150
OFFSET {offset}
"""
    resposta = requests.get(graphdb_endpoint, 
                                params={'query': query},
                                headers={'Accept': 'application/sparql-results+json'})
        
    if resposta.status_code == 200:
        dados = resposta.json()['results']['bindings']
        list_emiter = []
        for dado in dados:
            list_emiter.append(dado["emit_name"]["value"])
        return render_template('frontend/lista-autores/index.html', np=int(np),pag=pag, dados=list_emiter)
        


@app.route('/autores/add', methods=['GET', 'POST'])
def create_autor():
    if request.method == 'POST':
        author_name = request.form['authorName']
        document = request.form['issuedDocument']
        id = author_name.replace(" ", "_")
        
        if document != "":
            query = f"""
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

INSERT DATA {{
  :{id} rdf:type :Emitter ;
              :emitted :{document};
              :emitter_name "{author_name}" .
}}
"""
        else:
            query = f"""
PREFIX : <http://rpcw.di.uminho.pt/2024/Diario_Replublica/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

INSERT DATA {{
  :{id} rdf:type :Emitter ;
           :emitter_name "{author_name}" .
}}
"""
        try:
            sparql = SPARQLWrapper(graphdb_endpoint + "/statements")
            sparql.setMethod('POST')
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            sparql.query().convert()
            return jsonify({"success": True, "message": "Autor adicionado com sucesso!"})
        except Exception as e:
            return jsonify({"success": False, "message": "Autor não foi adicionado!"})
    return render_template('frontend/inserir-autor/index.html')
    
    
if __name__ == '__main__':
    app.run(debug=True)