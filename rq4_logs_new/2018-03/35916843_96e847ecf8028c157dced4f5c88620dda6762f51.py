"""Cupom Fiscal"""

from datetime import date


class CLIController:
    def __init__(self, output):
        self._output = output
        self._produtos = []

    def ler_produtos(self):
        pergunta = 'S'
        while pergunta == 'S':
            descricao = input("Produto: ")
            valor = float(input("Valor: "))
            self._produtos.append({'descricao': descricao, 'valor':valor})
            pergunta = input("Deseja continuar cadastrando? ")
    
    def writeln(self, text):
        self._output.write(text + '\n')

    def escrever_linha_cerquilha(self):
        self.writeln("#"*35)

    def escrever_separador_sessao(self):
        self.writeln(f"#{'='*33}#")

    def imprimir_cabecalho(self):
        self.escrever_linha_cerquilha()
        self.writeln(f'# Sistema de Venda Sr. José       #')
        hoje = date.today()
        self.writeln(f'# Data Compra: {hoje.strftime("%d/%m/%Y"):<18.18} #')
        self.escrever_separador_sessao()

    def imprimir_produtos(self):
        self.writeln('# Produtos Comprados:             #')
        self.writeln('# Nº Item |   Nome   |   Valor    #')
        total = 0.0

        for indice, produto in enumerate(self._produtos):
            total += produto['valor']
            self.imprimir_item(produto, indice + 1)

        self.escrever_separador_sessao()
        self.writeln(f'# Total:               R$ {total:> 7.2f} #')
        self.escrever_separador_sessao()
    
    def imprimir_item(self, produto, indice):
        self.writeln(f'# {indice:<8}|{produto["descricao"]:^10.10}| '
                     f'R$ {produto["valor"]:>7.2f} #')
    
    def imprimir_rodape(self):
        self.writeln('# Obrigado Volte Sempre           #')
        self.writeln('# Venda Sr. José agradece         #')
        self.escrever_separador_sessao()
        self.writeln('# DOJO Interprises SA             #')
        self.escrever_linha_cerquilha()

    def imprimir_cupom(self):
        self.imprimir_cabecalho()
        self.imprimir_produtos()
        self.imprimir_rodape()

if __name__ == "__main__":
    cupom = open("cupom_fiscal.txt", "w")
    controller = CLIController(cupom)
    controller.ler_produtos()
    controller.imprimir_cupom()