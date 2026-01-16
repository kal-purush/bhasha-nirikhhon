#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Import requests (Para download da página web)
# Para utilizar este import deve ser executado o seguinte comando:
# Linux (Debian/Ubuntu): sudo apt-get install python-requests
# Ou pelo pip: sudo pip install requests
import requests

# Import BeautifulSoup (Para fazer o parser do download)
from bs4 import BeautifulSoup

# Import Time (Para escolher de quanto em quanto tempo o script vai rodar)
import time

# Import smtplib (Para enviar email)
import smtplib

# Este é um simples script que faz o download de uma página web e monitora algum texto e nos envia um email.
# Caso ele não encontre o texto ele ira realizar o download da página novamente para a comparação
# Créditos: http://chrisalbon.com/python/monitor_a_website.html

# Loop eterno para sempre ficar verificando se existe uma alteração no site
while True:
    # Colocar a url a ser monitorada
    url = "http://Google.com/"
    # Colocando um cabeçalho para o script ser como um browser
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    # Faz o download da página
    response = requests.get(url, headers=headers)
    # Faz o parse da página toda pegando todos os textos dela
    soup = BeautifulSoup(response.text, "lxml")

    # Condição para ser disparada ou não nosso email
    if str(soup).find("Google") == -1:
        # Espera 60 segundos para realizar o proximo download e refazer a verificação,
        time.sleep(60)
        # continua o script após o tempo configurado
        continue

    # but if the word "Google" occurs any other number of times,
    else:
        # Cria o assunto do email a ser enviado
        msg = 'Assunto: Checagem de site e envio por email'
        # Configura o email do emitente
        fromaddr = 'meu_email@gmail.com'
        # Configura os destinatarios do email
        toaddrs  = ['AN_EMAIL_ADDRESS','A_SECOND_EMAIL_ADDRESS', 'A_THIRD_EMAIL_ADDRESS']

        # Configuração do servidor de email
        # server = smtplib.SMTP('smtp.gmail.com', 587)
        # server.starttls()
        # Adicionando as credenciais de conta de email
        # server.login("YOUR_EMAIL_ADDRESS", "YOUR_PASSWORD")

        # Printa o conteudo do email para testes
        print('From: ' + fromaddr)
        print('To: ' + str(toaddrs))
        print('Message: ' + msg)

        # Envio do email
        # server.sendmail(fromaddr, toaddrs, msg)
        # Desconecta do servidor de email
        # server.quit()

        break