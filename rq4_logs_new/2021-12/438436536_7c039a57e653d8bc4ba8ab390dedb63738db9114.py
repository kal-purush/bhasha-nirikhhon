from config import *
from modelo import Pessoa, Disciplina, EstudanteDisciplina

@app.route("/")
def inicio():
    return 'Sistema de cadastro de pessoas. '+\
        '<a href="/getPessoas">Listar</a>'

@app.route("/pessoa", methods=['get'])
def getPessoas():
    pessoas = db.session.query(Pessoa).all()
    pessoasJson = [ x.json() for x in pessoas ]
    resposta = jsonify(pessoasJson)
    resposta.headers.add("Access-Control-Allow-Origin", "*")
    return resposta

@app.route("/pessoa", methods=['post'])
def createPessoa():
    resposta = jsonify({"resultado": "ok", "detalhes": "created"})
    dados = request.get_json() 
    try: 
      nova = Pessoa(**dados) 
      db.session.add(nova) 
      db.session.commit() 
    except Exception as e: 
      resposta = jsonify({"resultado":"erro", "detalhes":str(e)})
    resposta.headers.add("Access-Control-Allow-Origin", "*")
    return resposta 


@app.route("/disciplina", methods=['get'])
def getDisciplina():
    disciplina = db.session.query(Disciplina).all()
    disciplinasJson = [ x.json() for x in disciplina ]
    resposta = jsonify(disciplinasJson)
    resposta.headers.add("Access-Control-Allow-Origin", "*")
    return resposta

@app.route("/disciplina", methods=['post'])
def createDisciplina():
    resposta = jsonify({"resultado": "ok", "detalhes": "created"})
    dados = request.get_json() 
    try: 
      nova = Disciplina(**dados) 
      db.session.add(nova) 
      db.session.commit() 
    except Exception as e: 
      resposta = jsonify({"resultado":"erro", "detalhes":str(e)})
    resposta.headers.add("Access-Control-Allow-Origin", "*")
    return resposta 

@app.route("/estudanteDisciplina", methods=['get'])
def getEstudanteDisciplina():
    estudanteDisciplina = db.session.query(EstudanteDisciplina).all()
    estudantesDiscipinasJson = [ x.json() for x in estudanteDisciplina ]
    resposta = jsonify(estudantesDiscipinasJson)
    resposta.headers.add("Access-Control-Allow-Origin", "*")
    return resposta

@app.route("/estudanteDisciplina", methods=['post'])
def createEstudanteDisciplina():
    dados = request.get_json() 
    resposta = jsonify({"resultado": "ok", "detalhes": dados})
    try: 
      nova = EstudanteDisciplina(**dados) 
      db.session.add(nova) 
      db.session.commit() 
    except Exception as e: 
      resposta = jsonify({"resultado":"erro", "detalhes":str(e)})
    resposta.headers.add("Access-Control-Allow-Origin", "*")
    return resposta 

app.run(debug=True)  