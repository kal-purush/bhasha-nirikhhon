import pandas as pd; pd.set_option('display.max_columns', 500); pd.set_option('display.max_rows', 100)
import numpy as np
from bs4 import BeautifulSoup as bs
import requests
from tqdm import tqdm
import datetime

def main():
    # Leo el csv actual como df para tener los id viejos y no scrapearlos 2 veces
    df_old = pd.read_csv('Argenprop_scrape.csv', dtype={'id': str}, parse_dates=['fecha_scrape'])

    # Scrape de nuevas publicaciones
    # Tope de páginas a scrapear

    pages = 50

    data = []
    done = False
    for i in range(1, pages + 1):

        response = requests.get("https://www.argenprop.com/departamento-alquiler-localidad-capital-federal-orden-masnuevos-pagina-" + str(i), headers={'User-Agent': 'Chrome'})
        soup = bs(response.text, 'html5lib')
        listings = soup.find_all('div', class_=lambda x:x=='listing__item' if x else False)

        if done:
            if len(data) == 0:
                print("No se encontraron publicaciones nuevas.")
                return
            else:
                para_imprimir_al_final = f'Se encontraron {len(data)} publicaciones nuevas.'
            break

        print(f'Scrapeando la página {i}...')
            
        # Por cada publicación encontrada, extraer los datos y appendear a la lista "data"
        for listing in listings:

            
            
            id_ = listing.find('a')

            # Chequear que sea una publicación válida, si no lo es, pasar a la siguiente
            if id_:
                id_ = id_.get('data-item-card')
                # Chequear si el id de la publicación ya está en el csv. Si ya está, termina el scrape
                if id_ in df_old.id.values:
                    done = True
                    break
            else:
                continue

            link = 'argenprop.com' + listing.find('a').get('href')
            direccion = listing.find(class_="card__address").text.strip()
            titulo = listing.find(class_="card__title").text.strip()
            descripcion = listing.find(class_="card__info").text.strip()

            ubicacion = listing.find(class_="card__title--primary").text[28:].split(', ')

            # La ubicación es de tipo "Capital Federal, barrio" o "barrio, sub_barrio"
            if ubicacion[-1] == 'Capital Federal':
                barrio = ubicacion[0]
                sub_barrio = np.nan
            else:
                barrio = ubicacion[-1]
                sub_barrio = ubicacion[0]

            precio = listing.find(class_="card__price").text.strip().split(' ')[1]

            moneda = listing.find(class_="card__currency")
            moneda = moneda.text.strip() if moneda else np.nan

            expensas = listing.find(class_="card__expenses")
            expensas = expensas.text.strip() if expensas else np.nan

            superficie = listing.find(class_="icono-superficie_cubierta")
            superficie = superficie.find_parent().find('span').text.strip() if superficie else np.nan

            dormitorios = listing.find(class_="icono-cantidad_dormitorios")
            dormitorios = dormitorios.find_parent().find('span').text.strip() if dormitorios else np.nan

            antiguedad = listing.find(class_="icono-antiguedad")
            antiguedad = antiguedad.find_parent().find('span').text.strip() if antiguedad else np.nan

            banos = listing.find(class_="icono-cantidad_banos")
            banos = banos.find_parent().find('span').text.strip() if banos else np.nan

            data.append([id_, link, direccion, titulo, descripcion, barrio, sub_barrio, precio, moneda, expensas, superficie, dormitorios, antiguedad, banos])

    # Creo el df a partir de la lista de listas "data".
    df = pd.DataFrame(data=data, columns='id,link,direccion,titulo,descripcion,barrio,sub_barrio,precio,moneda,expensas,superficie,dormitorios,antiguedad,banos'.split(','))

    # Lo invierto; las publicaciones más nuevas van abajo de todo.
    df = df.loc[::-1]

    # Agregar fecha y hora de scrape
    df['fecha_scrape'] = datetime.datetime.today()

    # df antes de ser limpiado
    print(df.tail())

    # Limpieza
    # Convierto los números a Int64 que acepta NaNs (primero los convierto a float para evitar un bug).
    df.precio = df.precio.str.replace('.', '', regex=False).apply(lambda x:np.nan if x[-1] != '0' else x).astype(float).astype(pd.Int64Dtype())
    df.expensas = df.expensas.apply(lambda x:x.split('\n')[0].split('$')[-1].replace('.', '') if type(x) == str else np.nan).astype(float).astype(pd.Int64Dtype())
    df.superficie = df.superficie.apply(lambda x:x.split(' ')[0].replace(',', '.') if type(x)==str else np.nan).astype(float)
    df.dormitorios = df.dormitorios.dropna().apply(lambda x:int(x[0]) if x[0] != 'M' else 0).astype(float).astype(pd.Int64Dtype())
    df.antiguedad = df.antiguedad.apply(lambda x:x.split(' ')[0] if type(x)==str else np.nan).str.replace('A', '0').astype(float).astype(pd.Int64Dtype())
    df.banos = df.banos.dropna().apply(lambda x:x[0]).astype(float).astype(pd.Int64Dtype())

    # Resultado final antes de ser exportado
    print(df.tail())

    print(para_imprimir_al_final)

    # Appendear 
    df.to_csv(f'Argenprop_scrape.csv', mode='a', index=False, header=False)

    return
    
if __name__ == '__main__':
    main()
    
input('El script terminó exitosamente. Presione Enter para finalizar...')