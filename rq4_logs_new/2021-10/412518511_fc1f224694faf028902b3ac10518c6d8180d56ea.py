import matplotlib.pyplot as plt
import numpy as np

x = np.linspace(0, 20, 100)  # Cria uma lista de números com espaçamento uniforme ao longo do intervalo
plt.plot(x, np.sin(x))       # Traçar o seno de cada ponto x
plt.show()                   # Exibir o gráfico