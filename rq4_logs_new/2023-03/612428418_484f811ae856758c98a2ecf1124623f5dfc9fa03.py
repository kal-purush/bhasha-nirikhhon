import datetime as dt
from datetime import datetime
import streamlit as st
import pandas as pd
import numpy as np
import random
import string
import warnings
warnings.filterwarnings('ignore')
import os
import glob
import gc
import requests
from bs4 import BeautifulSoup
from lxml import etree
import time
import re
from io import BytesIO

def convert_df_2_csv(df):
    try:
        return df.to_csv().encode('UTF-8-SIG')
    except:
        return df.to_csv().encode('ISO-8859-1')
    
def convert_df_2_excel(df):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Resultados del scraping')
    writer.save()
    processed_data = output.getvalue()
    return processed_data

def webscraping():
    my_bar = st.progress(0)
    # Variables del contador
    contador = 1
    longitud = len(iaItemListWithLink)
    contador_porcentaje = contador/longitud * 100
    start_time = time.time()

    for i in names_of_columns:
        iaItemListWithLink[i] = ''

    for i in iaItemListWithLink.ItemUPC:
        # Funciones del scraping      
        url = iaItemListWithLink['Links'][iaItemListWithLink['ItemUPC']==i].values[0]
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'}
        response = requests.get(url, headers=headers) #timeout=None
        soup = BeautifulSoup(response.content, 'html.parser')
        lxml_soup = etree.HTML(str(soup))
        for j in range(0,number_of_columns):
            try:
                results.append(lxml_soup.xpath(xpath_of_columns[j])[0])
            except:
                results.append('NAN')
            iaItemListWithLink[names_of_columns[j]][iaItemListWithLink['ItemUPC']==i] = results[j]
        
        # Funciones del contador
        time_of_exec = round(time.time(),0) - round(start_time,0)
        remaining_time = ((longitud-contador)*time_of_exec)/contador
        my_bar.progress(int(round(contador_porcentaje, 1)), text=str(contador) + ' de ' + str(longitud) + '   |   ' + str(round(contador_porcentaje, 1)) + '%' + '   |   ' + 'tiempo restante: ' + str(dt.timedelta(seconds=round(remaining_time,0))))
        contador = contador + 1
        contador_porcentaje = contador/longitud * 100
    st.write('\n')
    st.success('¡Webscraping terminado!')
    st.write('\n')

file_extensions = ['CSV', 'Excel']
iaItemListWithLink = None
names_of_columns = []
xpath_of_columns = []
results = []

st.title('Webscraping fácil')
st.subheader('Cargua tu archivo de datos')
file_type = st.selectbox(
                'Selecciona el tipo de archivo:',
                file_extensions)
if(file_type=='CSV'):
    uploaded_file = st.file_uploader("Cargua tu archivo",label_visibility="hidden")
    if(uploaded_file is not None):
        try:
            iaItemListWithLink = pd.read_csv(uploaded_file, encoding='UTF-8-SIG')
            st.dataframe(iaItemListWithLink.astype('str'))
        except:
            iaItemListWithLink = pd.read_csv(uploaded_file, encoding='ISO-8859-1')
            st.dataframe(iaItemListWithLink.astype('str'))
if(file_type=='Excel'):
    uploaded_file = st.file_uploader("Cargua tu archivo",label_visibility="hidden")
    excel_sheet = st.text_input(label='Escribe el nombre o número de la página que contiene los enlaces', value='')
    if(uploaded_file is not None and excel_sheet!=''):
        iaItemListWithLink = pd.read_excel(uploaded_file, sheet_name=excel_sheet)
        st.dataframe(iaItemListWithLink.astype('str'))

if(iaItemListWithLink is not None):
    column_of_code = st.text_input(label='Escribe el nombre de la columna que contiene SKU ó UPC', value='')
    column_of_link = st.text_input(label='Escribe el nombre de la columna que contiene los enlaces', value='')
    if(column_of_code != '' and column_of_link!=''):
        iaItemListWithLink = iaItemListWithLink.rename(columns={column_of_code:'ItemUPC', column_of_link:'Links'})
        number_of_columns = st.number_input('¿Cuántos datos quieres obtener?', value=0, step=1)
        for i in range(0,number_of_columns):
            names_of_columns.append(st.text_input(label='Escribe el nombre de la columna '+str(i+1), value=''))
        for i in range(0,number_of_columns):
            xpath_of_columns.append(st.text_input(label='Escribe el xpath de la columna '+str(i+1), value=''))
        init_ws = st.button('Iniciar')
        if init_ws == True:
            st.subheader('Webscraping')
            webscraping()
            st.subheader('Resultados')
            st.dataframe(iaItemListWithLink)

            csv_file = convert_df_2_csv(iaItemListWithLink)
            st.download_button(
                label="Descargar CSV",
                data=csv_file,
                file_name='webscraping_'+ re.sub("\.[a-zA-Z]+$", '',uploaded_file.name) +'.csv',
                mime='text/csv',
            )

            excel_file = convert_df_2_excel(iaItemListWithLink)
            st.download_button(
                label="Descargar excel",
                data=excel_file,
                file_name='webscraping_'+ re.sub("\.[a-zA-Z]+$", '',uploaded_file.name) +'.xlsx',
                mime='application/vnd.ms-excel'
            )