from kivy.uix.screenmanager import Screen
from kivy.properties import ObjectProperty
from kivy.lang import Builder
import sqlite3
import os

Builder.load_file(os.path.join(os.path.dirname(__file__), 'estoque.kv'))


# ----- TELA CADASTRO -----
class Cadastro_Produto(Screen):
    produto_id = ObjectProperty(None)
    nome_produto = ObjectProperty(None)
    tipo_produto = ObjectProperty(None)
    preco_produto = ObjectProperty(None)
    quantidade = ObjectProperty(None)
    validade = ObjectProperty(None)
    enviar = ObjectProperty(None)
    def adicionar_banco(self):
        
        try:
            conexao = sqlite3.connect('BD/projeto.db')
            cursor = conexao.cursor()
            cursor.execute('''
                INSERT INTO produto (produto_id, nome_produto, tipo_produto, preco_produto)
                VALUES (?, ?, ?, ?)
            ''', (
                self.produto_id.text,
                self.nome_produto.text,
                self.tipo_produto.text,
                self.preco_produto.text
            ))
            cursor.execute('''
                INSERT INTO estoque (produto_id, qntd_produto, validade)
                VALUES (?, ?, ?)
            ''', (self.produto_id.text, self.quantidade.text, self.validade.text))
            conexao.commit()
            conexao.close()
            self.enviar.text = 'Cadastro realizado com sucesso!'
        except sqlite3.Error as e:
            self.enviar.text = f'Erro: {e}'