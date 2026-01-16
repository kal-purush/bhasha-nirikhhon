from PyQt5 import  uic,QtWidgets
from colorama import Cursor
import mysql.connector
import sqlite3
import pandas as pd
import xlwt
import openpyxl
# import win32com.client as win32

banco = sqlite3.connect('banco_cadastro.db') 
cursor = banco.cursor()
cursor.execute("CREATE TABLE IF NOT EXISTS cadastro_chamadas (`ID` INTEGER  NOT NULL PRIMARY KEY AUTOINCREMENT,`QSO` VARCHAR(45) NOT NULL,`DIA` VARCHAR(45) NOT NULL,`PREFIXO` VARCHAR(45) NOT NULL,`HORA_INICIO` VARCHAR(45) NOT NULL,`HORA_FIM` VARCHAR(45) NOT NULL,  `FREQ_ou_FAIXA` VARCHAR(45) NOT NULL,`FONIA_ou_CW` VARCHAR(45) NOT NULL,`RST_ENV` VARCHAR(45) NOT NULL,`RST_REC` VARCHAR(45) NOT NULL,`QSL_ENV` VARCHAR(45) NOT NULL,`QSL_REC` VARCHAR(45) NOT NULL,`OUTRAS_ANOTACOES` VARCHAR(500) NULL,`DATA_INS_TABELA` DATE NULL)")
banco.commit() 
banco.close()

def inserir():
    tela_inserir.show()

def voltar_inserir():
    tela_inserir.close()

def alterar():
    tela_alterar.show()

def voltar_alterar():
    tela_alterar.close()

def excluir():
    tela_excluir.show()

def voltar_excluir():
    tela_excluir.close()

def inserir_dados():
    linha1 = tela_inserir.lineEdit_4.text()
    linha2 = tela_inserir.lineEdit_2.text()
    linha3 = tela_inserir.lineEdit_3.text()
    linha4 = tela_inserir.lineEdit_5.text()
    linha5 = tela_inserir.lineEdit_6.text()
    linha6 = tela_inserir.lineEdit_7.text()
    linha7 = tela_inserir.lineEdit_8.text()
    linha8 = tela_inserir.lineEdit_9.text()
    linha9 = tela_inserir.lineEdit_10.text()
    linha10 = tela_inserir.lineEdit_11.text()
    linha11 = tela_inserir.lineEdit_12.text()
    linha12 = tela_inserir.lineEdit_13.text()
    print(type(linha1))
    print(linha1)

    banco_inserir = sqlite3.connect('banco_cadastro.db') 
    cursor = banco_inserir.cursor()
    cursor.execute("INSERT INTO cadastro_chamadas (QSO,DIA,PREFIXO,HORA_INICIO,HORA_FIM,FREQ_ou_FAIXA,FONIA_ou_CW,RST_ENV,RST_REC,QSL_ENV,QSL_REC,OUTRAS_ANOTACOES) VALUES ('"+linha1+"','"+linha2+"','"+linha3+"','"+linha4+"','"+linha5+"','"+linha6+"','"+linha7+"','"+linha8+"','"+linha9+"','"+linha10+"','"+linha11+"','"+linha12+"')")
    banco_inserir.commit()
    banco_inserir.close()

    tela_inserir.lineEdit_4.setText("")
    tela_inserir.lineEdit_2.setText("")
    tela_inserir.lineEdit_3.setText("")
    tela_inserir.lineEdit_5.setText("")
    tela_inserir.lineEdit_6.setText("")
    tela_inserir.lineEdit_7.setText("")
    tela_inserir.lineEdit_8.setText("")
    tela_inserir.lineEdit_9.setText("")
    tela_inserir.lineEdit_10.setText("")
    tela_inserir.lineEdit_11.setText("")
    tela_inserir.lineEdit_12.setText("")    
    tela_inserir.lineEdit_13.setText("")   
    atualizar_db() 

def excluir_linha():
    id_excluir = tela_excluir.lineEdit.text()
    banco_excluir = sqlite3.connect('banco_cadastro.db')
    cursor = banco_excluir.cursor()
    cursor.execute("DELETE FROM cadastro_chamadas WHERE id='"+id_excluir+"'")
    banco_excluir.commit()
    banco_excluir.close()
    tela_excluir.lineEdit.setText("")   
    atualizar_db() 

def gerar_excel():
    banco_excel = sqlite3.connect('banco_cadastro.db')
    cursor = banco_excel.cursor()
    comando_SQL_excel = "SELECT * FROM cadastro_chamadas"
    cursor.execute(comando_SQL_excel)
    dados_lidos_excel = cursor.fetchall()
    print(dados_lidos_excel)
    try:
        for i in range(len(dados_lidos_excel)):
            d = {'Q S O \nNº.': [str(dados_lidos_excel[i][1])], 
            'DATA': [str(dados_lidos_excel[i][2])], 
            'PREFIXO': [str(dados_lidos_excel[i][3])],
            'HORA \nINÍCIO': [str(dados_lidos_excel[i][4])],
            'HORA \n FIM': [str(dados_lidos_excel[i][5])],
            'FREQ. \n ou \nFAIXA': [str(dados_lidos_excel[i][6])],
            'FONIA \n   ou \n  CW': [str(dados_lidos_excel[i][7])],
            'RST\nENV.': [str(dados_lidos_excel[i][8])],
            'RST \nREC.': [str(dados_lidos_excel[i][9])],
            'Q S L \n ENV.': [str(dados_lidos_excel[i][10])],
            'Q S L \n REC.': [str(dados_lidos_excel[i][11])],
            'OUTRAS \nANOTAÇÕES': [str(dados_lidos_excel[i][12])]
            }

        dados = pd.DataFrame(data= d)
        dados.to_excel('chamadas_cadastradas.xls', index= False)
        print("Arquivo Excel gerado com sucesso!")
    except:
        pass
    
    banco_excel.close()

app=QtWidgets.QApplication([])
tela_principal=uic.loadUi(r'tela_principal.ui')
tela_inserir=uic.loadUi(r'inserir_dados.ui')
tela_alterar=uic.loadUi(r'atualizar_dados.ui')
tela_excluir=uic.loadUi(r'tela_excluir.ui')

# Botões
tela_principal.pushButton_2.clicked.connect(inserir)
tela_principal.pushButton_3.clicked.connect(alterar)
tela_principal.pushButton_4.clicked.connect(excluir)
tela_principal.pushButton_5.clicked.connect(gerar_excel)

tela_inserir.pushButton.clicked.connect(inserir_dados)
tela_inserir.pushButton_2.clicked.connect(voltar_inserir)

tela_alterar.pushButton_2.clicked.connect(voltar_alterar)

tela_excluir.pushButton.clicked.connect(excluir_linha)
tela_excluir.pushButton_2.clicked.connect(voltar_excluir)

def atualizar_db():
    banco_atualizar_db = sqlite3.connect('banco_cadastro.db') 
    cursor = banco_atualizar_db.cursor()
    cursor.execute("SELECT * FROM cadastro_chamadas")
    dados_lidos = cursor.fetchall()
    # Coloca os dados na tabela da interface
    tela_principal.tableWidget.setRowCount(len(dados_lidos))
    tela_principal.tableWidget.setColumnCount(13)
    for i in range(0, len(dados_lidos)):
        for j in range(0,13):
            tela_principal.tableWidget.setItem(i,j,QtWidgets.QTableWidgetItem(str(dados_lidos[i][j])))

atualizar_db()

tela_principal.show()
app.exec()