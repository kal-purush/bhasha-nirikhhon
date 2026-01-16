import sqlite3
import os
from kivy.uix.screenmanager import Screen
from kivy.lang import Builder
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label

class Estoque(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Carrega o arquivo KV
        Builder.load_file(os.path.join(os.path.dirname(__file__), 'estoque.kv'))

        # Busca produtos do banco
        conexao = sqlite3.connect('BD/projeto.db')
        cursor = conexao.cursor()
        cursor.execute('SELECT produto_id, nome_produto, tipo_produto, qntd_produto, preco_produto FROM produto, estoque')
        cursor.row_factory = sqlite3.Row # para acessar colunas pelo nome (ex: produto['nome_produto'])
        produtos = cursor.fetchall()
        conexao.close()

# para rodar o código como se fosse o principal
class EstoqueApp(App):
    def build(self):
        return Estoque()

if __name__ == '__main__':
    EstoqueApp().run()