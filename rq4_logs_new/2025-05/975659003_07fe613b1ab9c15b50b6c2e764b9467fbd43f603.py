from kivy.app import App
from kivy.uix.screenmanager import ScreenManager, Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.properties import ObjectProperty
from kivy.lang import Builder
import sqlite3
import os

Builder.load_file(os.path.join(os.path.dirname(__file__), 'tela_cadastro_cliente.kv'))

class Tela_Cadastro_Cliente(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    procurar = ObjectProperty(None)

    def buscar_dados(self):
        conexao = sqlite3.connect('BD/projeto.db')
        cursor = conexao.cursor()
        cursor.execute("""
        SELECT * FROM cliente;
        """)
        dados = cursor.fetchall()
        conexao.close()
        return dados

    def atualizar_dados(self):
        container = self.ids.container
        container.clear_widgets()
        for cliente_cpf, telefone1, telefone2, email in self.buscar_dados():
            texto = f"CPF: {cliente_cpf} | Telefone 1: {telefone1} | Telefone 2: {telefone2} | Email: {email}"
            container.add_widget(Label(text=texto))