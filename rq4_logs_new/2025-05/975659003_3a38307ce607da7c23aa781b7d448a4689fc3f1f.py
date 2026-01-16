from kivy.app import App
from kivy.uix.screenmanager import ScreenManager, Screen
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.properties import ObjectProperty
from kivy.lang import Builder
import sqlite3

class Tela_Cadastro_Cliente(Screen):
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

class Cadastro_Cliente(Screen):
    cliente_cpf = ObjectProperty(None)
    tel1 = ObjectProperty(None)
    tel2 = ObjectProperty(None)
    email = ObjectProperty(None)
    enviar = ObjectProperty(None)
     
    def validar_cpf(self, cpf):
        cpf = ''.join(filter(str.isdigit, cpf))

        if len(cpf) != 11 or cpf == cpf[0] * 11:
            return False

        for i in [9, 10]:
            soma = sum(int(cpf[j]) * ((i + 1) - j) for j in range(i))
            digito = (soma * 10 % 11) % 10
            if digito != int(cpf[i]):
                return False
        return True
    
    def validar_telefone(self, telefone):
        telefone = ''.join(c for c in telefone if c.isdigit())
        return len(telefone) in [10, 11]
    
    def adicionar_banco(self):
        cliente_cpf_validacao = self.cliente_cpf.text
        tel1_validacao = self.tel1.text
        tel2_validacao = self.tel2.text
        email_validacao = self.email.text

        if not self.validar_cpf(cliente_cpf_validacao):
            self.enviar.text = 'CPF inválido!'
            return
        
        if not self.validar_telefone(tel1_validacao):
            self.enviar.text = 'Telefone 1 inválido!'
            return
        
        if tel2_validacao and not self.validar_telefone(tel2_validacao):
            self.enviar.text = 'Telefone 2 inválido!'
            return
        
        try:
            conexao = sqlite3.connect('BD/projeto.db')
            cursor = conexao.cursor()
            cursor.execute('''
                INSERT INTO cliente (cliente_cpf, telefone1, telefone2, email)
                VALUES (?, ?, ?, ?)
            ''', (cliente_cpf_validacao, tel1_validacao, tel2_validacao, email_validacao))
            conexao.commit()
            conexao.close()
            self.enviar.text = 'Cadastro realizado com sucesso!'
            
        except sqlite3.Error as e:
            self.enviar.text = f'Erro: {e}'
                    

class Gerenciador_Telas(App):
    def build(self):
        return Builder.load_file("tela_cadastro_cliente.kv")

if __name__ == '__main__':
    Gerenciador_Telas().run()