import os
import sqlite3
from kivy.app import App
from kivy.uix.gridlayout import GridLayout
from kivy.uix.widget import Widget
from kivy.properties import ObjectProperty

class MeuLayout(GridLayout):
    cpf = ObjectProperty(None)
    senha = ObjectProperty(None)
    enviar = ObjectProperty(None)

    def verificar(self):
        cpf = self.cpf.text
        senha = self.senha.text
        os.system('cls')
        print(f'CPF: {cpf}\nSenha: {senha}')

        conexao = sqlite3.connect("BD/projeto.db")
        cursor = conexao.cursor()
        cursor.execute("SELECT * FROM funcionario WHERE funcionario_cpf=? AND senha=?", (cpf, senha))
        resultado = cursor.fetchone()
        conexao.close()

        if resultado:
            self.enviar.text = "Login bem-sucedido!"
        else:
            self.enviar.text = "CPF ou senha inválidos."

class MeuApp(App):
    def build(self):
        return MeuLayout()

if __name__ == '__main__':
    MeuApp().run()