# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 09:53:02 2022

@author: Davi Fiquer
"""

from PIL import Image 
from PIL import ImageFont 
from PIL import ImageDraw 
import datetime
import noticias

def limpar_noticias(dados):
    quantidade = (len(dados) / 6)  # quantidade de noticias
    while True:
        horario = dados[0]  # dado especifico para o horario da
        pais = dados[2]  # dado especifico para o pais da noticia
        impacto = dados[3]  # dado especifico para o impacto da noticia
        chamada = dados[5]  # dado especifico para a chamada da noticia
        if impacto == 2 or impacto == 3:
            if impacto == 2:
                impacto_str = 'Médio'
            else:
                impacto_str = 'Alto'
            lista_noticias.append({'horario': horario, 'pais': pais,
                                   'noticia': chamada, 'impacto': impacto_str})
        for item in range(0, 6):
            del dados[0]  # apaga as ultimas informaçoes ja usadas(6 primeiros itens na lista), para nao ter repetiçoes
        quantidade = quantidade - 1
        if quantidade == 0:
            break
        else:
            pass

if __name__ == '__main__':
    lista_noticias = []
    limpar_noticias(noticias.calendario('https://br.investing.com/economic-calendar/'))
    # Carregando a imagem
    img = Image.open("template_news.png") 
    
    # ImageDraw
    draw = ImageDraw.Draw(img) 
    # Selecionando a fonte
    font = ImageFont.truetype("calibri.ttf", 22) 
    # Escrevendo a Data na imagem
    data = datetime.date.today()
    data = data.strftime('%d/%m/%Y') # Formatando a data para DD/MM/YYYY
    data_dia = 'Data: {}'.format(data)
    draw.text((433, 375), data_dia, (255, 255, 255), font=font) 
    # Escrevendo na imagem
    altura = 550
    altura_step = 50
    for news in lista_noticias:
        # print('Horário: {}, País: {}, Notícia: {}, Impacto: {}'.format(news['horario'], news['pais'], news['noticia'], news['impacto']))
        draw.text((47, altura), news['horario'], (255, 255, 255), font=font) # Hora
        draw.text((155, altura), news['pais'], (255, 255, 255), font=font) # País
        draw.text((355, altura), news['noticia'], (255, 255, 255), font=font) # Notícia
        draw.text((945, altura), str(news['impacto']), (255, 255, 255), font=font) # Impacto
        altura += altura_step
    # Salvando a nova imagem
    img.save('noticias_dia.jpg')