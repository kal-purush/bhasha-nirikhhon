import sqlite3
from kivy.app import App
from kivy.uix.gridlayout import GridLayout
from kivy.properties import ObjectProperty


class Cadastro_Produto(GridLayout):
    produto_id = ObjectProperty(None)
    nome_produto = ObjectProperty(None)
    tipo_produto = ObjectProperty(None)
    preco_produto = ObjectProperty(None)
    enviar = ObjectProperty(None)
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.tipo_contrato = None
       
    def adicionar_banco(self):
        produto_id_validacao = self.produto_id.text
        nome_produto_validacao = self.nome_produto.text
        tipo_produto_validacao = self.tipo_produto.text
        preco_produto_validacao = self.preco_produto.text
        
        try:
            conexao = sqlite3.connect('BD/projeto.db')
            cursor = conexao.cursor()
            cursor.execute('''
                INSERT INTO produto (produto_id, nome_produto, tipo_produto, preco_produto)
                VALUES (?, ?, ?, ?)
            ''', (produto_id_validacao, nome_produto_validacao, tipo_produto_validacao, preco_produto_validacao))
            conexao.commit()
            conexao.close()
            self.enviar.text = 'Cadastro realizado com sucesso!'
            
        except sqlite3.Error as e:
            self.enviar.text = f'Erro: {e}'
                    

class Cadastro_ProdutoApp(App):
    def build(self):
        return Cadastro_Produto()

if __name__ == '__main__':
    Cadastro_ProdutoApp().run()