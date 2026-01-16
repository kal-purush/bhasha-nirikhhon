#!/usr/bin/python3
import tkinter as tk
from tkinter import *
import locale
from nfe import download_json, download_png_path as nfe
from datetime import datetime
from scan_webcam import scan_bar_code

def set_text(entry, text):
	entry.delete(0,END)
	entry.insert(0,text)
	return

def processar_nfe():
	chave = chave_tk_entry.get()
	print("NF-E: %s" % chave)

	data = download_json(chave)
	if data is None:
		return

	serie=data["Dados da NF-e"]["Série"]
	numero=data["Dados da NF-e"]["Número"]

	dataEmissao=datetime.strptime(data["Dados da NF-e"]["Data de Emissão"], '%d/%m/%Y %H:%M:%S-%U:00')

	locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')
	valor=locale.atof(data["Dados da NF-e"]["Valor Total da Nota Fiscal"])

	set_text(serie_tk_entry, serie)
	set_text(numero_tk_entry, str(numero))
	set_text(data_tk_entry, dataEmissao)
	set_text(valor_tk_entry, str(valor))

def scanear_code():
	chave = scan_bar_code()
	set_text(chave_tk_entry, chave)
	processar_nfe()

master = tk.Tk()

tk.Label(master, text="Chave NF-E:").grid(row=0)

chave_tk_entry = tk.Entry(master)
chave_tk_entry.grid(row=0, column=1)

tk.Button(master, 
          text='Scanear Web Cam', 
          command=scanear_code).grid(row=1,
                                    column=0, 
                                    sticky=tk.W, 
                                    pady=4)
tk.Button(master, 
          text='Processar', 
          command=processar_nfe).grid(row=1,
                                    column=1, 
                                    sticky=tk.W, 
                                    pady=4)

tk.Label(master, text="Serie:").grid(row=2)
serie_tk_entry = tk.Entry(master)
serie_tk_entry.grid(row=2, column=1)

tk.Label(master, text="Número:").grid(row=3)
numero_tk_entry = tk.Entry(master)
numero_tk_entry.grid(row=3, column=1)

tk.Label(master, text="Data da Emissão:").grid(row=4)
data_tk_entry = tk.Entry(master)
data_tk_entry.grid(row=4, column=1)

tk.Label(master, text="Valor Total da Nota:").grid(row=5)
valor_tk_entry = tk.Entry(master)
valor_tk_entry.grid(row=5, column=1)

tk.mainloop()