import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split


# Uso de regresion logistica para predecir diabetes en mujeres mayores de 21 años.


#Lectura CSV//dataset
data = pd.read_csv('./dataset/diabetes2.csv')

#Separacion del dataset de entrenamiento
x_train,x_test,y_train,y_test = train_test_split(data.drop('Outcome', axis=1), data['Outcome'])

#Declaración del modelo como regresión logistica haciendo uso de sklearn, utilizamos el solver por defecto que hemos puesto de manera explicita y utilizamos 500 saltos enteros ya que 100 eran muy pocos para el modelo.
model = LogisticRegression(solver='lbfgs', max_iter= 500)

#Entrenamiento del modelo con la función fit de sklearn
model.fit(x_train,y_train)

# Realizamos predicciones para dos mujeres arriba de 21 años

#La primer mujer tiene un indice de glucosa alto al igual que el nivel de insulina sanguinea
print('Predicción para una mujer con: Embarazos: 0, Glucosa: 148, Presion en sangre: 72, Grosor de la piel: 35, Nivel de insulina: 83, IMC: 43, Función pedigree de diabetes: 0.521, Edad: 22 \n')
print(model.predict(np.array([[0,148,72,35,83,43,0.521,22]])) [0])

#La segunda mujer tiene un indice de glucosa regular al igual que el nivel de insulina sanguinea
print('Predicción para una mujer con: Embarazos: 0, Glucosa: 80, Presion en sangre: 72, Grosor de la piel: 35, Nivel de insulina: 70, IMC: 24, Función pedigree de diabetes: 0.521, Edad: 32 \n')
print(model.predict(np.array([[0,80,72,35,70,24,0.521,32]])) [0])


print('La exactitud del modelo es de: ')
print(model.score(x_test,y_test))