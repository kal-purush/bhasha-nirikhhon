import sqlite3
import pandas as pd
# Criar Banco

# cx = sqlite3.connect('Users.db')
# cc = cx.cursor()
# # Inserir Dados
# cc.execute(''' CREATE TABLE Usuarios (
#     email text,
#     usuario text,
#     senha text
#     )
# ''')
# cx.commit()
#
# cx.close()
#
# cb = sqlite3.connect('Tecnicos.db')
# ck = cb.cursor()
# # Inserir Dados
# ck.execute(''' CREATE TABLE Tecnico (
#         cpf text,
#         nome text,
#         telefone text,
#         turno text,
#         equipe text
#     )
# ''')
# cb.commit()
#
# cb.close()

#cb = sqlite3.connect('Feramentas.db')#ck = cb.cursor()
 # Inserir Dados
#ck.execute(''' CREATE TABLE Feramenta (
        #codigo text,
        #fabricante text,
        #voltagem text,
        #partnumber text,
        #descrição text,
        #tamanho text,
        #unidademed text,
        #tipo text,
        #material text
#     )
 #''')
# cb.commit()
#
# cb.close()





# Tecnicos -----------------------

conexao = sqlite3.connect('Tecnicos.db')
ck = conexao.cursor()

ck.execute("SELECT *, oid FROM Tecnico")
tecnicos_cadstrados = ck.fetchall()

conexao.commit()

conexao.close()

# Usuarios ----------------------

conx = sqlite3.connect('Users.db')

cursor = conx.cursor()

cursor.execute("SELECT *, oid FROM Usuarios")
usuarios_cadastrados = cursor.fetchall()

conx.commit()
conx.close()


feramentas= sqlite3.connect('Feramentas.db')

msg = feramentas.cursor()

msg.execute("SELECT *, oid FROM Feramenta")
feramenta_cadastrada = msg.fetchall()



feramentas.commit()


def exportar_ferramentas():
    feramentas= sqlite3.connect('Feramentas.db')

    msg = feramentas.cursor()

    msg.execute("SELECT *, oid FROM Feramenta")
    FRM = msg.fetchall()



    feramentas.commit()
    feramentas.close()