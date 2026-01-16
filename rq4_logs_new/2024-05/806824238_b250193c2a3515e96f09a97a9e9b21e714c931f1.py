# BIBLIOTECAS UTILIZADAS =====================================================
import customtkinter
from tkinter import messagebox
import webbrowser
import os

# FUNÇÕES ====================================================================
def confirmar():
    horas = entry_horas.get()
    if horas.isdigit():
        if horas.isdigit() >= 0 and horas.isdigit() <= 23:
            int_horas = int(horas)
    else:
        messagebox.showinfo("ATENÇÃO", "Informação incompatível\n\nVerifique novamente o campo HORAS.")
        entry_horas.delete(0, customtkinter.END)

    minutos = entry_minutos.get()
    if minutos.isdigit():
        if minutos.isdigit() >= 0 and minutos.isdigit() <= 59:
            int_minutos = int(minutos)
    else:
        messagebox.showinfo("ATENÇÃO", "Informação incompatível\n\nVerifique novamente o campo MINUTOS.")
        entry_minutos.delete(0, customtkinter.END)

    horas_convert = int_horas * 3600
    minutos_convert = int_minutos * 60
    tempo_total = horas_convert + minutos_convert
    if tempo_total == 0:
        tempo_total += 59
        os.system('shutdown -s -t {}'.format(tempo_total))
        messagebox.showinfo("INFORMAÇÃO", f"Sua máquina será desligada em menos de 1 minuto.\n\nVocê tem menos de 1 minuto para cancelar o agendamento.")
    elif tempo_total >= 60 and tempo_total <= 86399:
        os.system('shutdown -s -t {}'.format(tempo_total))
        messagebox.showinfo("INFORMAÇÃO", f"O seu equipamento será desligado em:\n\n{horas} hora(s) e {minutos} minuto(s).\n\nO programa será encerrado após clicar em OK.\n\nObrigado!")
        entry_horas.delete(0, customtkinter.END)
        entry_minutos.delete(0, customtkinter.END)
        janela.destroy()
    else:
        messagebox.showinfo('INFORMAÇÃO', 'Tempo mínimo é de 60 segundos.\n\nTempo máximo é de 23 horas e 59 minutos.')
        entry_horas.delete(0, customtkinter.END)
        entry_minutos.delete(0, customtkinter.END)

def limpar_campos():
    entry_horas.delete(0, customtkinter.END)
    entry_minutos.delete(0, customtkinter.END)

def cancelar():
    os.system('shutdown -a')
    messagebox.showinfo('INFORMAÇÃO', 'Agendamento cancelado.')
    entry_horas.delete(0, customtkinter.END)
    entry_minutos.delete(0, customtkinter.END)

# JANELA DO PROGRAMA =========================================================
customtkinter.set_appearance_mode("dark")
customtkinter.set_default_color_theme("dark-blue")
janela = customtkinter.CTk()
janela.title("Vai Já!")
janela.geometry('520x450+75+5')
janela.resizable(False, False)

# RÓTULOS E CAMPOS DE ENTRADA =================================================
texto_informativo = customtkinter.CTkLabel(janela, text="Informe nos campos a baixo\na quantidade de horas e minutos\npara o desligamento do seu computador.", font=("Candara", 15))
texto_informativo.place(x=140, y=30)
texto_informativo2 = customtkinter.CTkLabel(janela, text="Tempo máximo: 23 Horas e 59 Minutos", font=("Consoles", 20, "bold"), text_color="red")
texto_informativo2.place(x=75, y=105)

label_horas = customtkinter.CTkLabel(janela, text="Quantas horas?", font=("Arial", 15))
label_horas.place(x=138, y=160)
entry_horas = customtkinter.CTkEntry(janela, placeholder_text="Um número entre 0 e 23", font=("Arial", 10))
entry_horas.place(x=260, y=160)

label_minutos = customtkinter.CTkLabel(janela, text="Quantos minutos?", font=("Arial", 15))
label_minutos.place(x=130, y=195)
entry_minutos = customtkinter.CTkEntry(janela, placeholder_text="Um número entre 0 e 59", font=("Arial", 10))
entry_minutos.place(x=260, y=195)

# BOTÕES =====================================================================
botao_confirmar = customtkinter.CTkButton(janela, text="Confirmar", font=("Arial", 14, "bold"), cursor="hand2", command=confirmar).place(x=190, y=270)
botao_limpar_campos = customtkinter.CTkButton(janela, text="Limpar Campos", font=("Arial", 14, "bold"), cursor="hand2", command=limpar_campos).place(x=190, y=310)
botao_cancelar = customtkinter.CTkButton(janela, text="Cancelar", font=("Arial", 14, "bold"), cursor="hand2", command=cancelar).place(x=190, y=350)
# CRIADOR =====================================================================
criador = customtkinter.CTkLabel(janela, text=" 2024 - Criado por").place(x=5, y=420)
link = customtkinter.CTkLabel(janela, text="Evandro Oliveira", cursor="hand2", font=("Arial", 12, "underline", "italic"))
link.place(x=107, y=419)
link.bind('<Button-1>', lambda x:webbrowser.open_new("https://github.com/evandro-dev23/"))
janela.mainloop() 