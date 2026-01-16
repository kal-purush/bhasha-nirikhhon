from kivy.app import App
from kivy.uix.screenmanager import ScreenManager, Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.properties import ObjectProperty
from kivy.lang import Builder
import sqlite3

# ----- TELA ESTOQUE -----
class Tela_Estoque(Screen):
    def buscar_dados(self):
        conexao = sqlite3.connect('BD/projeto.db')
        cursor = conexao.cursor()
        cursor.execute("""
        SELECT 
            produto.nome_produto,
            produto.tipo_produto,
            produto.preco_produto,
            estoque.qntd_produto,
            estoque.validade
        FROM produto
        LEFT JOIN estoque ON produto.produto_id = estoque.produto_id;
        """)
        dados = cursor.fetchall()
        conexao.close()
        return dados

    def atualizar_dados(self):
        container = self.ids.container
        container.clear_widgets()
        for nome, tipo, preco, qntd, validade in self.buscar_dados():
            texto = f"Produto: {nome} | Categoria: {tipo} | Preço: R${preco:.2f} | Quantidade: {qntd} | Validade: {validade}"
            container.add_widget(Label(text=texto))

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

# ----- SCREEN MANAGER APP -----
class Gerenciador(App):
    def build(self):
        return Builder.load_file("telas.kv")

if __name__ == '__main__':
    Gerenciador().run()