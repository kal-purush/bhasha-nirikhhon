import pandas as pd

#Dirección del archivo csv
rutaFileCsv = 'https://github.com/luisguillermomolero/MisionTIC2022_2/blob/master/Modulo1_Python_MisionTIC2022_Main/Semana_5/Reto/movies.csv?raw=true'

def listaPeliculas(link: str) -> str:
    try:
        datos = pd.read_csv(link)
    except:
        print('Error al leer el archivo de datos')
    else:
        # print(datos)
        subset = datos[['Country', 'Language', 'Gross Earnings']]
        tabla = subset.pivot_table(index = ['Country', 'Language'])        
        return tabla.head(10)
        
        
        
print(listaPeliculas(rutaFileCsv))