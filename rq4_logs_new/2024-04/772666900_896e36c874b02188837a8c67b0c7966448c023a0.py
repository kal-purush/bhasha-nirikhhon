import os
import zipfile
import pandas as pd
from os import scandir, getcwd

import os
import glob

directorio_inicial='data'

def obtener_estructura_con_rutas(input_directory):
    estructura = {}
    for elemento in os.listdir(input_directory):
        ruta_elemento = os.path.join(input_directory, elemento)
        if os.path.isdir(ruta_elemento):
            estructura[elemento] = obtener_estructura_con_rutas(ruta_elemento)
        else:
            estructura[elemento] = ruta_elemento
    return estructura

estructura_directorios_txt_con_rutas = obtener_estructura_con_rutas(directorio_inicial)

def etl(valor,i):
    keys_segundarias = list(estructura_directorios_txt_con_rutas[valor].keys())

    archivos_txt_completos = list(estructura_directorios_txt_con_rutas[valor][keys_segundarias[i]].values())

    # Lista para almacenar los DataFrames de cada archivo
    dataframes = []

    # Leer cada archivo y cargarlo en un DataFrame
    for archivo_completo in archivos_txt_completos[1:]:
        df = pd.read_csv(archivo_completo, sep='\t', header=None)  # Puedes ajustar los parámetros según el formato de tus archivos
        dataframes.append(df)

    # Concatenar todos los DataFrames en uno solo
    dataframe_concatenado = pd.concat(dataframes, ignore_index=True)
    dataframe_concatenado.columns= ['phrase']
    dataframe_concatenado['target'] = keys_segundarias[i]
    return dataframe_concatenado


tabla = [] 
for i in range(1,4):
    tabla.append(etl('train',i))
pd.concat(tabla, ignore_index=True).to_csv('train_dataset.csv')


tabla = [] 
for i in range(1,4):
    tabla.append(etl('test',i))
pd.concat(tabla, ignore_index=True).to_csv('test_dataset.csv')