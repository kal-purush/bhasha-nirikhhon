from fpdf import FPDF
from datetime import date
from utils.format_utils import valor_por_extenso
import os

# Entrada de dados
cliente = input("Informe o nome do cliente: ").strip()
consulta = input("Informe o tipo da consulta: ").strip()
valor = float(input("Informe o valor da consulta: ").replace(",", ".").strip())

# Processamento
valor_extenso = valor_por_extenso(valor)
valor_formatado = f"{valor:.2f} reais"
data_atual = date.today()
dia = data_atual.day
mes = data_atual.month
ano = data_atual.year

# Criação do PDF
pdf = FPDF()
pdf.add_page()
pdf.set_font("Arial", "", 20)

# Imagem de fundo
pdf.image("dados/rec.jpg", x=0, y=0)

# Inserção dos dados
pdf.text(162, 45, valor_formatado)
pdf.text(70, 86, cliente)
pdf.text(70, 108, valor_extenso)
pdf.text(70, 135, consulta)

pdf.set_text_color(255, 255, 255)
pdf.text(30, 193, str(dia))
pdf.text(50, 193, str(mes))
pdf.text(70, 193, str(ano))

# Geração do arquivo
nome_arquivo = f"{cliente.replace(' ', '_')}_{dia}_{mes}_{ano}.pdf"
caminho_arquivo = os.path.join("recibos", nome_arquivo)
pdf.output(caminho_arquivo)

print(f"✅ Recibo gerado com sucesso em: {caminho_arquivo}")