#!/usr/bin/env python
# coding: utf-8

# # Dados obtidos em www.simcosta.furg.br

# In[2]:


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime, timedelta

data_path = os.getcwd() + '/dados' # precisa limpar o cabecalho desses arquivos


# In[3]:


def passa_data(data):
    return datetime.strptime(data, '%Y %m %d %H %M %S')


# In[4]:


def trata_dado(path):
    dado = pd.read_csv(path)
    
    Ano = dado['YEAR'].apply(str)
    Mes = dado['MONTH'].apply(str)
    Dia = dado['DAY'].apply(str)
    Hora = dado['HOUR'].apply(str)
    Minuto = dado['MINUTE'].apply(str)
    Segundo = dado['SECOND'].apply(str)
    dado['DATA'] = (Ano + ' ' + Mes + ' ' + Dia + ' ' + Hora + ' ' + Minuto + ' ' + Segundo).apply(passa_data)
    dado = dado.set_index('DATA')
    del(dado['YEAR'])
    del(dado['MONTH'])
    del(dado['DAY'])
    del(dado['HOUR'])
    del(dado['MINUTE'])
    del(dado['SECOND'])
    
    return dado


# In[5]:


def teste_serie_temporal(df, delta_time = 24*7):
    counter = 0
    falhas = []
    for i in range(len(df.index)):
        if df.index[i] > df.index[i-1] + timedelta(hours=delta_time):
            falhas.append((df.index[i-1], df.index[i]))
#             print('Inicio da falha temporal: ' + str(df.index[i-1]))
#             print('Fim da falha na serie temporal ' + str(df.index[i]))
            counter += 1
    print('Foram encontradas ' + str(counter) +' falhas na serie temporal.')
    return falhas



# In[ ]:


# botar isso mais pra cima

def aproveitamento(dataframe, variavel):

    total = dataframe.notnull().sum()[variavel]
    total_tempo = dataframe.notnull().sum()['EE_'+variavel]
    flag = dataframe.notnull().sum()['jump_flag']
    crisis = dataframe.notnull().sum()['jump_crisis']
    perc_flag = round(flag*100/total, 2)
    perc_crisis = round(crisis*100/total, 2)
    perc_datas = round(total*100/total_tempo, 2)

    print("De %s valores, %s foram marcados com a flag de pulo (%s%s).\n%s valores foram marcados com a flag de crise (%s%s)."
          % (str(total), str(flag),str(perc_flag), '%', str(crisis), str(perc_crisis), "%"))
          
          
def evento_extremo(dataframe, variavel = 'Hsig'):
    media = dataframe.mean()
    desvpad = dataframe.std()
    evento_extremo = media + 4*desvpad
    
    evento_extremo_line = np.zeros(len(dataframe))
    for i in range(len(evento_extremo_line)):
        evento_extremo_line[i] = evento_extremo[variavel]
    
    dataframe['EE_' + variavel] = evento_extremo_line
    
    
def jump_flag(dataframe, variavel ,desvpad):

    col_jump_flag = np.empty(len(dataframe[variavel]))
    col_jump_flag[:] = np.nan


    for i in range(len(dataframe[variavel])):
        if abs(dataframe[variavel][i-1] - dataframe[variavel][i]) > desvpad[variavel]: # a diferenca do valor extremo eh um desvpad
            col_jump_flag[i] = dataframe[variavel][i]
        #print(i)
        
    dataframe['jump_flag'] = col_jump_flag
    
    
def jump_crisis(dataframe, variavel ,desvpad):

    col_jump_crisis = np.empty(len(dataframe[variavel]))
    col_jump_crisis[:] = np.nan


    for i in range(len(dataframe[variavel])):
        if abs(dataframe[variavel][i-1] - dataframe[variavel][i]) > 2*desvpad[variavel]: # a diferenca do valor extremo eh um desvpad
            col_jump_crisis[i] = dataframe[variavel][i]
        
    dataframe['jump_crisis'] = col_jump_crisis
    
    
def evento_extremo(dataframe, variavel = 'Hsig', jf = True, jc = True):
    media = dataframe.mean()
    desvpad = dataframe.std()
    evento_extremo = media + 4*desvpad
    
    evento_extremo_line = np.zeros(len(dataframe))
    for i in range(len(evento_extremo_line)):
        evento_extremo_line[i] = evento_extremo[variavel]
    
    dataframe['EE_' + variavel] = evento_extremo_line
    
    if jf:
        jump_flag(dataframe, variavel, desvpad)
    if jc:
        jump_crisis(dataframe, variavel, desvpad)