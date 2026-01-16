import speedtest
from customtkinter import *
import requests


def conectado():
    try:
        requests.get('https://www.google.com', timeout=5)
        return 'Conectado'
    except:
        return 'Desconectado'


def testar():
    try:
        status.configure(text='Calculando...')
        janela.update()
        teste = speedtest.Speedtest()
        velocidade_de_download = teste.download()
        velocidade_de_upload = teste.upload()
        ping = teste.results.ping

        status.configure(text='Concluído')
        ldownload.configure(text= f'Velocidade de Download: {velocidade_de_download / 10**6:.2f} Mbps')
        lupload.configure(text= f'Velocidade de Upload: {velocidade_de_upload / 10**6:.2f} Mbps')
        lping.configure(text=f'Ping: {ping:.2f} ms')
    except:
        status.configure(text='ERRO!')


janela = CTk()
janela.title('Speed Test')
janela.geometry('346x550+500+50')
janela.resizable(width=False, height=False)

titulo = CTkLabel(janela, text='Teste a velocidade da sua internet', font=('Arial', 20, 'bold'), text_color='#05AFF2')
titulo.grid(row=0, column=0, padx=10, pady=20)

descricao = CTkLabel(janela, text='Para testar a velocidade de download, upload e\n o ping de sua internet, basta clicar no \nbotão "Testar" abaixo.')
descricao.grid(row=1, column=0, rowspan=2, padx=10, pady=10)

status = CTkLabel(janela, text='')
status.grid(row=5, column=0, padx=10, pady=10)

botao = CTkButton(janela, text='Testar', command=testar)
botao.grid(row=6, column=0, padx=10, pady=15)

frame = CTkFrame(janela, corner_radius=10)
frame.grid(row=7, column=0, rowspan=2, columnspan=2, sticky='nswe', padx=10, pady=35)

ldownload = CTkLabel(frame, text='')
ldownload.grid(row=0, column=0, sticky='w', padx=5)

lupload = CTkLabel(frame, text='')
lupload.grid(row=1, column=0, sticky='w', padx=5)

lping = CTkLabel(frame, text='')
lping.grid(row=2, column=0, sticky='w', padx=5)

conexao = CTkLabel(janela, text= f'Status de Conexão: {conectado()}', text_color='#05AFF2')
conexao.grid(row=9, column=0, sticky='w', padx=5)

janela.mainloop()

#Desenvolvido por Igor Souza.
#GitHub - https://github.com/Isouz