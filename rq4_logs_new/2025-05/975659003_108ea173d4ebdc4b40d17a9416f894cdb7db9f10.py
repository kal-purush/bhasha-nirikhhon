import sqlite3
from kivy.app import App
from kivy.uix.gridlayout import GridLayout
from kivy.properties import ObjectProperty


class Cadastro_Usuario(GridLayout):
    nome = ObjectProperty(None)
    cpf = ObjectProperty(None)
    senha = ObjectProperty(None)
    confirmar_senha = ObjectProperty(None)
    enviar = ObjectProperty(None)
    salario= ObjectProperty(None)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def adicionar_banco(self):
        conexao = sqlite3.connect("BD/projeto.db")
        cursor = conexao.cursor()
        # cursor.execute("SELECT * FROM funcionario WHERE funcionario_cpf=? AND senha=?", (cpf, senha))
        resultado = cursor.fetchone()
        conexao.close()
        
class CadastroApp(App):
    def build(self):
        return Cadastro_Usuario()

if __name__ == '__main__':
    CadastroApp().run()