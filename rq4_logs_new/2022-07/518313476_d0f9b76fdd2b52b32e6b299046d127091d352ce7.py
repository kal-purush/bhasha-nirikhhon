import pyautogui #tem que baixar amigao
import pyperclip
import time
import pandas as pd


pyautogui.PAUSE = 1
#Abrir uma nova aba no navegador
pyautogui.hotkey("ctrl", "t")
pyautogui.write("https://drive.google.com/drive/folders/14oLE59U1RqyRqlBbKpsyymW-mitvbtoh")# link do arquivo em excel
pyautogui.press("enter")

#selecionar o arquivo e baixar
time.sleep(5)
pyautogui.position()
pyautogui.click(x=368,y=351)
pyautogui.click(x=1169, y=195)
pyautogui.click(x=918, y=551)
time.sleep(7)
#calcular uma coluna da tabela
tabela = pd.read_excel(r"C:\Users\alexd\Downloads\Vendas - Dez.xlsx") #diretorio do arquivo salvo
quantidade = tabela["Quantidade"].sum()
faturamento = tabela["Valor Final"].sum()
mensagem = f"""Bom dia senhora, 
este é a quantidade vendida de produtos: ["quatidade"] 
este é o faturamento em R$: [faturamento]"""
#Entrar no email para enviar a mensagem 
pyautogui.hotkey("ctrl", "t")
pyautogui.write("https://mail.google.com/mail/u/0/#inbox") #link do email
pyautogui.press("enter")
time.sleep(5)
#Enviar o email]
pyautogui.click(x=79, y=232)
time.sleep(2)
pyautogui.write("alexdudo13@gmail.com")# email destinatario
pyautogui.press("tab", 2)
pyperclip.copy("Relatório de vendas")
pyautogui.hotkey("ctrl", "v")
pyautogui.press("tab")
mensagem = f"""Bom dia senhora, 
esta é a quantidade vendida de produtos: {quantidade:,}
este é o faturamento em R$: {faturamento:,.2f}"""
pyperclip.copy(mensagem)
pyautogui.hotkey("ctrl", "v")
pyautogui.hotkey("ctrl", "enter")