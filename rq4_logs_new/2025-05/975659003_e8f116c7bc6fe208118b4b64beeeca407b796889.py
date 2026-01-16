from kivy.app import App
from kivy.uix.screenmanager import ScreenManager, Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.properties import ObjectProperty
from kivy.lang import Builder
import sqlite3

class Tela_Cadastro_Cargo(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
    def buscar_dados(self):
        conexao = sqlite3.connect('BD/projeto.db')
        cursor = conexao.cursor()
        cursor.execute("""
            SELECT * FROM cargo
        """)

        dados = cursor.fetchall()
        conexao.close()
        return dados

    def atualizar_dados(self):
        container = self.ids.container
        container.clear_widgets()
        for cargo_id, nome_cargo, tipo_contrato, setor_id in self.buscar_dados():
            texto = f"ID: {cargo_id} | Cargo: {nome_cargo} | Tipo contrato: {str(tipo_contrato).upper()} | ID do Setor: {setor_id}"
            container.add_widget(Label(text=texto))

class Cadastro_Cargo(Screen):
    cargo_id = ObjectProperty(None)
    nome_cargo = ObjectProperty(None)
    setor_id = ObjectProperty(None)
    enviar = ObjectProperty(None)
        
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.tipo_contrato = None

    def set_tipo_contrato(self, tipo, ativo):
        if ativo:
            self.tipo_contrato = tipo
            
    def adicionar_banco(self):
        cargo_id_validacao = self.cargo_id.text
        nome_cargo_validacao = self.nome_cargo.text
        tipo_contrato_validacao = self.tipo_contrato
        setor_id_validacao = self.setor_id.text
        
        try:
            conexao = sqlite3.connect('BD/projeto.db')
            cursor = conexao.cursor()
            cursor.execute('''
                INSERT INTO cargo (cargo_id, nome_cargo, tipo_contrato, setor_id)
                VALUES (?, ?, ?, ?)
            ''', (cargo_id_validacao, nome_cargo_validacao, tipo_contrato_validacao, setor_id_validacao))
            conexao.commit()
            conexao.close()
            self.enviar.text = 'Cadastro realizado com sucesso!'
            
        except sqlite3.Error as e:
            self.enviar.text = f'Erro: {e}'
    
class Gerenciador_Telas(App):
    def build(self):
        return Builder.load_file("Tela_Cadastro_Cargo.kv")

if __name__ == '__main__':
    Gerenciador_Telas().run()