#Resumen del py : 
#1. Leer datos: Leer archivo .csv (origen de datos)
#2. Extraer el resumen solicitado, para este caso es la cantidad de valores unicos de todas las columnas
#3. Guardar el resumen en formato csv. 

from pkgutil import get_data
import pandas as pd
import os 
from pathlib import Path 

def main():
    root_dir=Path(".").resolve()
    print("root dir es: ",root_dir)
    #Leer Archivo
    data=get_data(filename="llamadas123_julio_2022.csv")
    #Extraer resumen
    df_resumen = get_summary(data)
    #Guardar Resumen
    save_data(df_resumen, filename="llamadas123_julio_2022.csv")

def save_data(df, filename):
    root_dir=Path(".").resolve()
    out_name = 'resumen3_' + filename
    out_path = os.path.join(root_dir, 'data','processed',out_name)
    df.to_csv(out_path)


def get_summary(data):
    dict_resumen = dict()
    for col in data.columns:
        valores_unicos=data[col].unique() 
        n_valores= len(valores_unicos) 
    
        dict_resumen[col] = n_valores

    df_resumen = pd.DataFrame.from_dict(dict_resumen, orient='index')
    df_resumen.rename ({0:'Count'},axis=1, inplace=True)

    return df_resumen


def get_data (filename):
    data_dir = 'raw'
    root_dir=Path(".").resolve()
    file_path=os.path.join(root_dir,"data",data_dir,filename)
    data = pd.read_csv(file_path, encoding='latin-1', sep =';')
    return data

if __name__ == '__main__':
    main()