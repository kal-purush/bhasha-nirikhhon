import sqlite3
from kivy.app import App
from kivy.uix.gridlayout import GridLayout
from kivy.properties import ObjectProperty


class Cadastro_Cargo(GridLayout):
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
        print("ID:", cargo_id_validacao)
        print("Cargo:", nome_cargo_validacao)
        print("Tipo:", tipo_contrato_validacao)
        print("Setor ID:", setor_id_validacao)
        
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
                    

class Cadastro_CargoApp(App):
    def build(self):
        return Cadastro_Cargo()

if __name__ == '__main__':
    Cadastro_CargoApp().run()