# Importa as bibliotecas
import os
import os.path
import collections
import smtplib
import mimetypes
import email
import xlrd
import email.mime.application
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.utils import COMMASPACE, formatdate
from time import sleep

try:
    # Dados para fazer o Login
    host = 'smtp.hostinger.com'
    port = 587
    user = 'ti@2ori.com.br'
    password = 'R3n4n@gl3c1'

    # Criando objeto para conexão
    print('Iniciando programa...')
    print('')
    print('Criando objeto servidor...')
    print('')
    server = smtplib.SMTP(host, port)

    # Login com servidor
    print(f'Realizando o login em {host}...')
    print('\n')
    server.ehlo()
    server.starttls()
    server.login(user, password)

    # Criando mensagem

    pasta = os.listdir('I:/imagens digitalizadas/Livro 2/')
    imagens_ausentes = list()
 
    print("PASTAS INCOMPLETAS:")
    print("===================")
    for imagem in pasta:
        pasta_das_imagens = os.listdir('I:/imagens digitalizadas/Livro 2/' + str(imagem))
        lista_imagens = list()
        lista_matriculas = list()
        lista_comparacao = list()
        for string_imagens in pasta_das_imagens:
            if string_imagens != 'Thumbs.db' and string_imagens != 'Sl1MWYEG.jpg':
                num_letras = string_imagens[:8]
                num_inteiros = int(num_letras)
                lista_imagens.append(num_inteiros)
            else:
                pass
        for matriculas_num_inteiros in lista_imagens:
            if matriculas_num_inteiros not in lista_matriculas:
                lista_matriculas.append(matriculas_num_inteiros)
            else:
                pass
        primeira = lista_matriculas[0]
        ultima = lista_matriculas[-1]
        for lista_numeros_gerados_para_comparacao in range(primeira, ultima+1):
            lista_comparacao.append(lista_numeros_gerados_para_comparacao)

        if(collections.Counter(lista_matriculas) == collections.Counter(lista_comparacao)):
            pass
        else:
            print('I:/imagens digitalizadas/Livro 2/' + str(imagem), end=" => ")
            print("Imagens Incompletas")
            for imagem_matricula in range(primeira, ultima+1):
                if imagem_matricula not in lista_matriculas:
                    imagens_ausentes.append(imagem_matricula)
                else:
                    pass
except NotADirectoryError:
    print("")
    print("MATRÍCULAS INCOMPLETAS:")
    print("======================")
    for i in imagens_ausentes:
        print(i, end=", ")
except ValueError:
    print("")
    print("MATRÍCULAS INCOMPLETAS")
    print("======================")
    arquivo = open('C:/Auditor/imagens ausentes.txt', 'w')
    arquivo.write("MATRÍCULAS INCOMPLETAS")
    arquivo.write("\n")
    arquivo.write("======================")
    arquivo.write("\n")
    for i in imagens_ausentes:
        print(i, end=", ")
        arquivo.write(f'{i} \n')
        arquivo.write("\n")
    arquivo.close()
else:
    print("")

print("\n")




# Anexa os arquivos e envia o e-mail
try:
    message = f'Olá, este é um auditor automático da pasta de imagens que são enviadas para a Central RISC no diretório "I\:". Após verificação de rotina, notei que e as seguintes matrículas não estão presentes na pasta de imagens:\n\n{imagens_ausentes}.\n\n Verifique com o setor responsável ou com a Escriba para solução do problema. \n\n Gratos \n\n Setor de TI'
    msg = MIMEMultipart()
    msg['From'] = 'ti@2ori.com.br'
    msg['To'] = 'ti@2ori.com.br'
    msg['Subject'] = 'Imagens ausentes'
    msg.attach(MIMEText(message, 'plain'))
    server.sendmail(msg['From'], msg['To'], msg.as_string())
    print(f'Mensagem enviada para ti@2ori.com.br!')
    print('\n')
except Exception as erro:
    print(f'ERRO no envio dos emails: ')
    print(f'{erro}')
# Encerra o envio dos e-mails   
else:
    sleep(1.0)
    print("Programa encerrado!!!")
    server.quit()





    
