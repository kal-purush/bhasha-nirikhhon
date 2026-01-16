import numpy as np
import pandas as pd
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score

# Carregar o conjunto de dados
data = load_breast_cancer()
X = pd.DataFrame(data.data, columns=data.feature_names)
y = data.target

# Dividir o conjunto de dados em treinamento e teste
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

train_scores = []
test_scores = []

# Testar diferentes profundidades máximas
for depth in range(1, 21):
    # Criar o classificador da árvore de decisão
    clf = DecisionTreeClassifier(max_depth=depth, random_state=42)
    
    # Treinar o modelo
    clf.fit(X_train, y_train)
    
    # Calcular a acurácia no conjunto de treinamento
    train_accuracy = accuracy_score(y_train, clf.predict(X_train))
    train_scores.append(train_accuracy)
    
    # Calcular a acurácia no conjunto de teste
    test_accuracy = accuracy_score(y_test, clf.predict(X_test))
    test_scores.append(test_accuracy)

# Plotar as curvas de aprendizado
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.plot(range(1, 21), train_scores, label='Treinamento', marker='o')
plt.plot(range(1, 21), test_scores, label='Teste', marker='o')
plt.xlabel('Profundidade máxima da árvore de decisão')
plt.ylabel('Acurácia')
plt.title('Curvas de Aprendizado: Árvore de Decisão')
plt.legend()
plt.grid(True)
plt.show()