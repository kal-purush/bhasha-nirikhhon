from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn import svm
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score, f1_score
from sklearn.metrics import precision_score
from pickle import dump, load

class Classificador():
    def __init__(self):
        self.vetorizador = None
        self.revisoes_teste = None
        self.recomendacoes_teste = None
        self.classificador = None
        self.modelo = None
        self.caminho_arquivo = None
        self.nome = None
        self.preditor = None

    def salvar(self):
        with open(self.caminho_arquivo, "wb") as arquivo:
            dump([self.revisoes_teste, self.recomendacoes_teste, self.modelo], arquivo)

    def carregar(self):
        with open(self.caminho_arquivo, "rb") as arquivo:
            self.revisoes_teste, self.recomendacoes_teste, self.modelo = load(arquivo)
            self.setar_preditor()

    def marcador(self):
        acuracia = accuracy_score(self.recomendacoes_teste, self.preditor)
        precisao = precision_score(self.recomendacoes_teste, self.preditor, average="macro")
        fmeasure = f1_score(self.recomendacoes_teste, self.preditor, average="macro")
        return (("%.6f   %.6f   %.6f"%(acuracia, precisao, fmeasure)))

    def setar_preditor(self):
        self.preditor = self.modelo.predict(self.revisoes_teste)

    def preditor_texto(self, texto):
        texto_processado = self.vetorizador.vetorizar_texto(texto)
        return self.modelo.predict(texto_processado)


class NaiveBayes(Classificador):
    def __init__(self, caminho_arquivo, vetorizador = None, recomendacoes = None):
        self.vetorizador = vetorizador
        self.recomendacoes = recomendacoes
        self.caminho_arquivo = caminho_arquivo
        self.nome = "NaiveBayes/MultinomialNB"

        try:
            print("\nTentando carregar o classificador %s..."%self.nome)
            self.carregar()
            print("O classificador foi carregado.")
        except:
            print("A tentativa falhou. Treinando...")
            self.criar()
            self.setar_preditor()
            self.salvar()
            print("O classificador %s foi salvo."%self.nome)

    def criar(self):
        revisoes_processadas = self.vetorizador.revisoes_processadas
        revisoes_treino, self.revisoes_teste, recomendacoes_treino, self.recomendacoes_teste = train_test_split(revisoes_processadas, self.recomendacoes, test_size=0.2, random_state=0)
        classificador = MultinomialNB()
        classificador.fit(revisoes_treino, recomendacoes_treino)
        self.modelo = classificador


class SVM(Classificador):
    def __init__(self, caminho_arquivo, vetorizador = None, recomendacoes = None):
        self.vetorizador = vetorizador
        self.recomendacoes = recomendacoes
        self.classificador = svm.SVC()
        self.caminho_arquivo = caminho_arquivo
        self.nome = "SVM/GridSearchCV"

        try:
            print("\nTentando carregar o classificador %s..."%self.nome)
            self.carregar()
            print("O classificador foi carregado.")

        except:
            print("A tentativa falhou. Treinando...")
            self.criar()
            self.setar_preditor()
            self.salvar()
            print("O classificador %s foi salvo."%self.nome)

    def criar(self):
        revisoes_processadas = self.vetorizador.revisoes_processadas
        hyper = {"C": [1.0, 2.0, 3.0],
                 "kernel": ["linear", "rbf", "sigmoid", "poly"],
                 "degree": [2, 3, 4],
                 "gamma": ["scale"]}
        revisoes_treino, self.revisoes_teste, recomendacoes_treino, self.recomendacoes_teste = train_test_split(revisoes_processadas, self.recomendacoes, test_size=0.2, random_state=0)
        classificador = GridSearchCV(self.classificador, hyper, cv = 5, n_jobs = 1)
        classificador.fit(revisoes_treino, recomendacoes_treino)
        self.modelo = classificador

    