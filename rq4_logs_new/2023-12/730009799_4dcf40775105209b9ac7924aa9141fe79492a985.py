# Importando bibliotecas necessárias
from ast import List
from flask_openapi3 import OpenAPI, Info, Tag
from flask import jsonify, redirect, request
from schemas import *
from flask_cors import CORS
import json
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score
from sklearn.pipeline import Pipeline
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC

# Configurando informações básicas para a documentação da API
info = Info(title="Machine Learning", version="1.0.0")
app = OpenAPI(__name__, info=info)
CORS(app)

# Definindo tags para organização da documentação
home_tag = Tag(name="Documentação", description="Seleção de documentação: Swagger, Redoc ou RapiDoc")
machine_learning_tag = Tag(name="Machine Learning", description="Endpoint de processamento de dataset")

# Rota principal, redireciona para a escolha do estilo de documentação
@app.get('/', tags=[home_tag])
def home():
    """Redireciona para /openapi, tela que permite a escolha do estilo de documentação."""
    return redirect('/openapi')

# Rota para processar um arquivo de dados e realizar operações de Machine Learning
@app.post('/processar-arquivo', tags = [machine_learning_tag])
def processar_arquivo():
    try:
        # Obtendo dados da requisição
        arquivo, campo_saida, inputs = obter_dados_requisicao(request)
        # Carregando o conjunto de dados
        dataset = carregar_dataset(arquivo)
        # Processando os dados e obtendo saídas
        saidas = processar(dataset, campo_saida, inputs)
        saidas = np.array(saidas).tolist()
        resultado = {'saidas': saidas}
        return jsonify(resultado), 200

    except Exception as e:
        print(e)
        return jsonify({'error': str(e)}), 500

# Função auxiliar para obter dados da requisição
def obter_dados_requisicao(request):
    if 'file' not in request.files:
        return jsonify({'error': 'Nenhum arquivo enviado'}), 400

    arquivo = request.files['file']
    campo_saida = request.form['saida'].replace('"', '')
    inputs = request.form['inputs']

    if not inputs:
        return jsonify({'error': 'Forneça os dados a serem testados'}), 400

    return arquivo, campo_saida, inputs

# Função para carregar um conjunto de dados a partir de um arquivo
def carregar_dataset(arquivo):
    if arquivo.filename == '' or not arquivo.filename.endswith('.csv'):
        return jsonify({'error': 'Arquivo inválido, deve ser um arquivo CSV'}), 400

    return pd.read_csv(arquivo)

# Função principal para processamento de dados e Machine Learning
def processar(dataset, campo_saida, inputs):
    print(inputs)
   
    # Preparando dados para treinamento
    X, y = preparar_dados(dataset, campo_saida)
   
    # Dividindo dados em conjuntos de treinamento e teste
    X_train, X_test, y_train, y_test = dividir_dados(X, y)
   
    # Criando modelos de Machine Learning
    models, names = criar_modelos()
    
    # Avaliando desempenho dos modelos por validação cruzada
    avaliar_modelos(models, X_train, y_train, names)
   
    # Usando os hiperparâmetros do modelo KNN
    melhor_modelo = tunar_knn(X_train, y_train)
   
    # Avaliando desempenho do melhor modelo no conjunto de teste
    acuracia_teste, scaler = avaliar_modelo_teste(melhor_modelo, X_train, X_test, y_train, y_test)
   
    # Tratando inputs fornecidos e realizando previsões
    inputs_tratados = tratar_inputs(inputs)
   
    # Retorno das previvões com os dados fornecidos
    saidas = prever_saidas(melhor_modelo, inputs_tratados, scaler)
    
    return saidas

# Função para preparar dados para treinamento
def preparar_dados(dataset, campo_saida):
    X = dataset.drop([campo_saida], axis=1)
    y = dataset[campo_saida]
    return X, y

# Função para dividir dados em conjuntos de treinamento e teste
def dividir_dados(X, y):
    test_size = 0.20
    seed = 7
    return train_test_split(X, y, test_size=test_size, random_state=seed)

# Função para criar modelos de Machine Learning
def criar_modelos():
    models = [('KNN', KNeighborsClassifier()),
              ('CART', DecisionTreeClassifier()),
              ('NB', GaussianNB()),
              ('SVM', SVC())]
    names = [name for name, _ in models]
    return models, names

# Função para avaliar desempenho dos modelos por validação cruzada
def avaliar_modelos(models, X_train, y_train, names):
    results = []
    for name, model in models:
        cv_results = cross_val_score(model, X_train, y_train, cv=StratifiedKFold(n_splits=10, shuffle=True, random_state=7), scoring='accuracy')
        results.append(cv_results)
        msg = "%s: %f (%f)" % (name, cv_results.mean(), cv_results.std())
        print(msg)
    return results

# Função para sintonizar hiperparâmetros do modelo KNN
def tunar_knn(X_train, y_train):
    np.random.seed(7)
    knn = KNeighborsClassifier()
    standard_scaler = StandardScaler()
    min_max_scaler = MinMaxScaler()
    pipelines = [('knn-orig', Pipeline(steps=[('KNN', knn)])),
                 ('knn-padr', Pipeline(steps=[('StandardScaler', standard_scaler), ('KNN', knn)])),
                 ('knn-norm', Pipeline(steps=[('MinMaxScaler', min_max_scaler), ('KNN', knn)]))]
    param_grid = {'KNN__n_neighbors': [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21],
                  'KNN__metric': ["euclidean", "manhattan", "minkowski"]}
    melhor_modelo = ajustar_modelo(pipelines, param_grid, X_train, y_train)
    return melhor_modelo

# Função para ajustar modelo utilizando GridSearchCV
def ajustar_modelo(pipelines, param_grid, X_train, y_train):
    for name, model in pipelines:
        grid = GridSearchCV(estimator=model, param_grid=param_grid, scoring='accuracy', cv=StratifiedKFold(n_splits=10, shuffle=True, random_state=7), n_jobs=-1)
        grid.fit(X_train, y_train)
        print("Sem tratamento de missings: %s - Melhor: %f usando %s" % (name, grid.best_score_, grid.best_params_))
    return grid.best_estimator_

# Função para avaliar desempenho do modelo no conjunto de teste
def avaliar_modelo_teste(modelo, X_train, X_test, y_train, y_test):
    scaler = StandardScaler().fit(X_train)
    rescaledX = scaler.transform(X_train)
    modelo.fit(rescaledX, y_train)
    rescaledTestX = scaler.transform(X_test)
    predictions = modelo.predict(rescaledTestX)
    acuracia = accuracy_score(y_test, predictions)
    print(acuracia)
    return acuracia, scaler

# Função para prever saídas do modelo para inputs fornecidos
def prever_saidas(modelo, inputs_tratados, scaler):
    colunas = list(inputs_tratados.keys())
    entrada = pd.DataFrame(inputs_tratados, columns=colunas)
    array_entrada = entrada.values
    X_entrada = array_entrada[:, :array_entrada.size].astype(float)
    rescaledEntradaX = scaler.transform(X_entrada)
    saidas = modelo.predict(rescaledEntradaX)
    print(saidas)
    return list(saidas)

# Função para tratar inputs fornecidos
def tratar_inputs(json_str):
    try:
        inputs = json.loads(json_str)
        novo_objeto = {}
        for item in inputs:
            nome = item["name"]
            valores = [float(valor.strip()) if '.' in valor else int(valor.strip()) for valor in item["values"].split(",")]
            novo_objeto[nome] = valores
    except Exception as e:
        print(e)
    return novo_objeto