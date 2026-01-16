# importa as bibliotecas que vão ser utilizadas
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import time

# dividi em funções para manutenção mais fácil e reutilização em outras planilhas, se for preciso

def preencher_formulario(navegador, data):
    """
    parâmetro navegador: o objeto do navegador já instanciado
    parâmetro data: os dados que vão ser preenchidos no formulário, extraídos da planilha do Excel
    """
    for _, linha in data.iterrows():
        # itera sobre o índice e linha da planilha, chamando a função preencher_campos com os respectivos parâmetros
        preencher_campos(navegador, "input[ng-reflect-name='labelFirstName']", linha["First Name"])
        preencher_campos(navegador, "input[ng-reflect-name='labelLastName']", linha["Last Name "])
        preencher_campos(navegador, "input[ng-reflect-name='labelCompanyName']", linha["Company Name"])
        preencher_campos(navegador, "input[ng-reflect-name='labelRole']", linha["Role in Company"])
        preencher_campos(navegador, "input[ng-reflect-name='labelAddress']", linha["Address"])
        preencher_campos(navegador, "input[ng-reflect-name='labelEmail']", linha["Email"])
        preencher_campos(navegador, "input[ng-reflect-name='labelPhone']", linha["Phone Number"])
        # localiza e seleciona o botão de envio do formulário, clica nele, e submete o formulário
        navegador.find_element(By.CLASS_NAME, "btn.uiColorButton").click()

def preencher_campos(navegador, seletor, valor):
    """
    parâmetro navegador: o objeto do navegador já instanciado
    parâmetro seletor: o seletor CSS do campo que vai ser preenchido
    parâmetro valor: o valor que vai ser inserido no campo
    """
    campo = navegador.find_element(By.CSS_SELECTOR, seletor) # localiza o campo com o método find_element
    campo.send_keys(valor) # insere o valor no campo do formulário
    time.sleep(1) # aguarda um tempo para visualização e garantir que o campo foi preenchido

""" 
- ponto de entrada do programa: lê a tabela do Excel, inicia o navegador, acessa a URL do desafio e maximiza a janela
- localiza e clica no botão de start do desafio e chama a função principal para preencher todo o formulário
- aguarda um tempo razoável para garantir que o processo foi concluído, e mostra o resultado antes de fechar o navegador
"""
if __name__ == "__main__":
    tabela = pd.read_excel("challenge.xlsx")
    navegador = webdriver.Chrome()
    navegador.get("https://www.rpachallenge.com/")
    navegador.maximize_window()
    navegador.find_element(By.CSS_SELECTOR, "button.waves-effect.col.s12.m12.l12.btn-large.uiColorButton").click()
    preencher_formulario(navegador, tabela)
    time.sleep(10)
    navegador.quit()