from itertools import count
from tkinter import *
from tkinter import ttk
from tkinter import messagebox
from tkinter import colorchooser
import sqlite3

janela_inicial = Tk()
janela_inicial.geometry("1000x650")
janela_inicial.title("Farmácia Inicial")
janela_inicial.resizable(False, False)
janela_inicial.iconbitmap(default="titulo.ico")
janela_inicial.configure(background="white")

#CARREGANDO IMAGENS
logo_farma = PhotoImage(file="logomarca.png")
icone_financeiro = PhotoImage(file="dinheiro_novo.png")
icone_funcionarios = PhotoImage(file="adm_novo.png")
icone_estoque = PhotoImage(file="estoque_novo.png")
icone_vendas = PhotoImage(file="vendas_novo.png")
icone_suporte = PhotoImage(file="suporte_novo.png")
icone_desliga = PhotoImage(file="desliga.png")

def muda_cor_primaria():
    #Pick color
    cor_primaria = colorchooser.askcolor()[1]
    #Atualiza a treeview
    if cor_primaria:
        #Criando striped row tags
        minha_arvore.tag_configure('evenrow', background=cor_primaria)

def muda_cor_secundaria():
    #Pick color
    cor_secundaria = colorchooser.askcolor()[1]
    #Atualiza a treeview
    if cor_secundaria:
        #Criando striped row tags
        minha_arvore.tag_configure('oddrow', background=cor_secundaria)
        
def muda_cor_destaque():
    #Pick color
    cor_destaque = colorchooser.askcolor()[1]
    #Atualiza a treeview
    if cor_destaque:
    #Mudando cores quando selecionado
        style.map('Treeview', background=[('selected', cor_destaque)])

def consulta_base_dados():
    #limpa tudo
    for record in minha_arvore.get_children():
        minha_arvore.delete(record)
    #Criando a base de dados ou se conectando a uma base existente
    conn = sqlite3.connect('farm_inic.db')
    #Criando uma instância do cursor
    c = conn.cursor()
    c.execute("SELECT rowid, * FROM tabela_medicamentos")
    records = c.fetchall()
    
    #Adicionando nossos dados na tela
    global count
    count = 0
    
    #for record in records:
    #    print(record)

    for record in records:
        if count % 2 == 0:
            minha_arvore.insert(parent='', index='end', iid=count, text='', values=(record[1], record[2], record[0], record[4], record[5], record[6], record[7]), tags=('evenrow', ))
        else:
            minha_arvore.insert(parent='', index='end', iid=count, text='', values=(record[1], record[2], record[0], record[4], record[5], record[6], record[7]), tags=('oddrow', )) 
        #incrementando contador
        count += 1

    #Salvar alterações
    conn.commit()
    #Fechar a conexão
    conn.close()

def buscador_regs():
    buscar_registro = entrada_busca.get()
    #fechar a janela de busca
    busca.destroy()
    #limpa tudo
    for record in minha_arvore.get_children():
        minha_arvore.delete(record)
    #Criando a base de dados ou se conectando a uma base existente
    conn = sqlite3.connect('farm_inic.db')
    #Criando uma instância do cursor
    c = conn.cursor()
    c.execute("SELECT rowid, * FROM tabela_medicamentos WHERE nome like ?", (buscar_registro,)) #"like" em vez de "=" para buscar resultados tanto em letra minúsculas ou maiúsculas
    records = c.fetchall()
    #Adicionando nossos dados na tela
    global count
    count = 0
    #for record in records:
    #    print(record)
    for record in records:
        if count % 2 == 0:
            minha_arvore.insert(parent='', index='end', iid=count, text='', values=(record[1], record[2], record[0], record[4], record[5], record[6], record[7]), tags=('evenrow', ))
        else:
            minha_arvore.insert(parent='', index='end', iid=count, text='', values=(record[1], record[2], record[0], record[4], record[5], record[6], record[7]), tags=('oddrow', )) 
        #incrementando contador
        count += 1
    #Salvar alterações
    conn.commit()
    #Fechar a conexão
    conn.close()


def busca_regs():
    global entrada_busca, busca
    busca = Toplevel(janela_inicial)
    busca.title("Busca de registros")
    busca.geometry("400x200")
    busca.iconbitmap(default="titulo.ico")
    #criando label frame
    frm_busca = LabelFrame(busca, text="Nome")
    frm_busca.pack(padx=10, pady=10)
    #adicionando entrada de usuário
    entrada_busca = Entry(frm_busca, font=("Helvetica", 18))
    entrada_busca.pack(pady=20, padx=20)
    #adicionando botão
    btn_busca = Button(busca, text="Buscar registros", command=buscador_regs)
    btn_busca.pack(padx=20, pady=20)

#Adicionando menu opções
meu_menu = Menu(janela_inicial)
janela_inicial.config(menu=meu_menu)

#configurando o menu
menu_opçoes = Menu(meu_menu, tearoff=0)
meu_menu.add_cascade(label="Opções", menu=menu_opçoes)
#drop down menu
menu_opçoes.add_command(label="Cor primária", command=muda_cor_primaria)
menu_opçoes.add_command(label="Cor secundária", command=muda_cor_secundaria)
menu_opçoes.add_command(label="Cor de destaque", command=muda_cor_destaque)
menu_opçoes.add_separator()
menu_opçoes.add_command(label="Sair", command=janela_inicial.quit)

#Adicionando menu busca
menu_busca = Menu(meu_menu, tearoff=0)
meu_menu.add_cascade(label="Busca", menu=menu_busca)
#drop down menu
menu_busca.add_command(label="Buscar registros", command=busca_regs)
menu_busca.add_separator()
menu_busca.add_command(label="Mostrar todos", command=consulta_base_dados)

"""#Adicionando dados fictícios
dados = [
    ["Neosaldina", "Takeda", 1, "Dipirona", "100121", "Mal estar", "25,49"],
    ["Kóide", "Momenta", 2, "Betametasona", "100122", "Anti-inflamatório", "39,19"],
    ["Clavulin", "GSK", 3, "Amoxicilina", "100123", "Antibiótico", "78,52"],
    ["Aceviton", "CIMED", 4, "Ácido ascórbico", "100124", "Vitaminas", "10,24"],
    ["Seakalm", "Natulab", 5, "Passiflora", "100125", "Calmantes", "14,67"],
    ["Biprofenid", "Sanofi", 6, "Cetoprofeno", "100126", "Anti-inflamatório", "53,85"],
    ["Iruxol", "Abbott", 7, "Colagenase", "100127", "Dermatologia", "62,94"],
    ["Omcilon-A Orabase", "Aspen", 8, "Triancinolona", "100128", "Dermatologia", "20,29"],
    ["Lufty", "Airela", 9, "Simeticona", "100129", "Antigases", "7,50"],
    ["PredSim", "Mantecorp", 10, "Prednisolona", "100130", "Anti-inflamatório", "30,39"],
    ["Neosaldina", "Takeda", 11, "Dipirona", "100121", "Mal estar", "25,49"],
    ["Kóide", "Momenta", 12, "Betametasona", "100122", "Anti-inflamatório", "39,19"],
    ["Clavulin", "GSK", 13, "Amoxicilina", "100123", "Antibiótico", "78,52"],
    ["Aceviton", "CIMED", 14, "Ácido ascórbico", "100124", "Vitaminas", "10,24"],
    ["Seakalm", "Natulab", 15, "Passiflora", "100125", "Calmantes", "14,67"],
    ["Biprofenid", "Sanofi", 16, "Cetoprofeno", "100126", "Anti-inflamatório", "53,85"],
    ["Iruxol", "Abbott", 17, "Colagenase", "100127", "Dermatologia", "62,94"],
    ["Omcilon-A Orabase", "Aspen", 18, "Triancinolona", "100128", "Dermatologia", "20,29"],
    ["Lufty", "Airela", 19, "Simeticona", "100129", "Antigases", "7,50"],
    ["PredSim", "Mantecorp", 20, "Prednisolona", "100130", "Anti-inflamatório", "30,39"]
]
"""

#---------------BASE DE DADOS--------------------
#Criando a base de dados ou se conectando a uma base existente
conn = sqlite3.connect('farm_inic.db')
#Criando uma instância do cursor
c = conn.cursor()

#Criar tabela
c.execute("""CREATE TABLE if not exists tabela_medicamentos (
    nome text,
    fabricante text,
    id integer,
    substancia text,
    lote text,
    secao text,
    preco text)
""")

"""#Adicionar base de dados fictícia à tabela
for record in dados:
    c.execute("INSERT INTO tabela_medicamentos VALUES (:nome, :fabricante, :id, :substancia, :lote, :secao, :preco)",
        {
            'nome': record[0],
            'fabricante': record[1],
            'id': record[2],
            'substancia': record[3],
            'lote': record[4],
            'secao': record[5],
            'preco': record[6]
        })
"""

#Salvar alterações
conn.commit()
#Fechar a conexão
conn.close()

#Adicionar um estilo
style = ttk.Style()

#Inserir um tema
style.theme_use('default')

#Configurando cores na treeview
style.configure("Treeview", background="#D3D3D3", foreground="black", rowheight=25, fieldbackground="#D3D3D3")

#Mudando cores quando selecionado
style.map('Treeview', background=[('selected', "#347083")])

#Criando frame para a treeview
tree_frame = Frame(janela_inicial)


#Criando barra de rolagem para treeview
tree_scroll = Scrollbar(tree_frame)
tree_scroll.pack(side=RIGHT, fill=Y)

#Criando a treeview em si
minha_arvore = ttk.Treeview(tree_frame, yscrollcommand=tree_scroll.set, selectmode="extended")
minha_arvore.pack()

#Configurando a barra de rolagem
tree_scroll.config(command=minha_arvore.yview)

#Definindo as colunas
minha_arvore['columns'] = ("Nome", "Fabricante", "ID", "Substância", "Lote", "Secao", "Preco")

#Formatando as colunas
minha_arvore.column("#0", width=0, stretch=NO)
minha_arvore.column("Nome", anchor=W, width=140)
minha_arvore.column("Fabricante", anchor=W, width=90)
minha_arvore.column("ID", anchor=CENTER, width=50)
minha_arvore.column("Substância", anchor=W, width=100)
minha_arvore.column("Lote", anchor=CENTER, width=70)
minha_arvore.column("Secao", anchor=CENTER, width=120)
minha_arvore.column("Preco", anchor=CENTER, width=70)

#Criando cabeçalhos
minha_arvore.heading("#0", text="", anchor=W)
minha_arvore.heading("Nome", text="Nome", anchor=W)
minha_arvore.heading("Fabricante", text="Fabricante", anchor=W)
minha_arvore.heading("ID", text="ID", anchor=CENTER)
minha_arvore.heading("Substância", text="Substância", anchor=W)
minha_arvore.heading("Lote", text="Lote", anchor=CENTER)
minha_arvore.heading("Secao", text="Secao", anchor=CENTER)
minha_arvore.heading("Preco", text="Preco", anchor=CENTER)

#Criando striped row tags
minha_arvore.tag_configure('oddrow', background="white")
minha_arvore.tag_configure('evenrow', background="lightblue")


#Adicionando gravações de dados de entrada
dados_frame = LabelFrame(janela_inicial, text="Dados", width=40, height=50)


lbl_nome = Label(dados_frame, text="Nome")
lbl_nome.grid(row=0, column=0, padx=5, pady=5)
txtbox_nome = Entry(dados_frame)
txtbox_nome.grid(row=0, column=1, padx=5, pady=5)

lbl_fabricante = Label(dados_frame, text="Fabricante")
lbl_fabricante.grid(row=0, column=2, padx=5, pady=5)
txtbox_fabricante = Entry(dados_frame)
txtbox_fabricante.grid(row=0, column=3, padx=5, pady=5)

lbl_ID = Label(dados_frame, text="ID")
lbl_ID.grid(row=0, column=4, padx=5, pady=5)
txtbox_ID = Entry(dados_frame, width=10)
txtbox_ID.grid(row=0, column=5, padx=5, pady=5)

lbl_substancia = Label(dados_frame, text="Substância")
lbl_substancia.grid(row=1, column=0, padx=5, pady=5)
txtbox_substancia = Entry(dados_frame)
txtbox_substancia.grid(row=1, column=1, padx=5, pady=5)

lbl_lote = Label(dados_frame, text="Lote")
lbl_lote.grid(row=1, column=4, padx=5, pady=5)
txtbox_lote = Entry(dados_frame, width=10)
txtbox_lote.grid(row=1, column=5, padx=5, pady=5)

lbl_secao = Label(dados_frame, text="Seção")
lbl_secao.grid(row=1, column=2, padx=6, pady=6)
txtbox_secao = Entry(dados_frame)
txtbox_secao.grid(row=1, column=3, padx=6, pady=6)

lbl_preco = Label(dados_frame, text="Preço")
lbl_preco.grid(row=1, column=6, padx=7, pady=7)
txtbox_preco = Entry(dados_frame, width=10)
txtbox_preco.grid(row=1, column=7, padx=7, pady=7)

#mover acima
def move_acima():
    rows = minha_arvore.selection()
    for row in rows:
        minha_arvore.move(row, minha_arvore.parent(row), minha_arvore.index(row)-1)

#mover abaixo
def move_abaixo():
    rows = minha_arvore.selection()
    for row in reversed(rows):
        minha_arvore.move(row, minha_arvore.parent(row), minha_arvore.index(row)+1)

#remover um registro
def remove_um():
    x = minha_arvore.selection()[0]
    minha_arvore.delete(x)

    #Criando a base de dados ou se conectando a uma base existente
    conn = sqlite3.connect('farm_inic.db')
    #Criando uma instância do cursor
    c = conn.cursor()

    #remover da base de dados
    c.execute("DELETE from tabela_medicamentos WHERE oid=" + txtbox_ID.get())

    #Salvar alterações
    conn.commit()
    #Fechar a conexão
    conn.close()
    #limpa as entradas
    limpa_entradas()

    #adicionando mensagem de confirmação
    messagebox.showinfo("REMOÇÃO", "O registro foi removido com sucesso.")

#remover vários registros
def remove_varios():
    #adicionando mensagem de confirmação
    resposta = messagebox.askyesno("REMOÇÃO", "Isso removerá OS REGISTROS SELECIONADOS da tabela.\nTem certeza?")
    #adicionando lógica à caixa de mensagem
    if resposta == 1:
        #designando as seleções
        x = minha_arvore.selection()

        #criar lista de IDs
        ids_para_remover = []

        #adicionar seleções na lista ids_para_remover
        for record in x:
            ids_para_remover.append(minha_arvore.item(record, 'values')[2]) #"[2]" é o terceiro item dentro da tupla onde estão os dados de cada item(remédio) da lista
        
        #remover da treeview
        for record in x:
            minha_arvore.delete(record)
        #Criando a base de dados ou se conectando a uma base existente
        conn = sqlite3.connect('farm_inic.db')
        #Criando uma instância do cursor
        c = conn.cursor()

        #remover vários registros a base de dados
        c.executemany("DELETE FROM tabela_medicamentos WHERE id = ?", [(a,) for a in ids_para_remover])

        #Salvar alterações
        conn.commit()
        #Fechar a conexão
        conn.close()
        #limpa as entradas
        limpa_entradas()

#remover todos os registros
def remove_todos():
    #adicionando mensagem de confirmação
    resposta = messagebox.askyesno("REMOÇÃO", "Isso removerá TODOS OS REGISTROS da tabela.\nTem certeza?")
    #adicionando lógica à caixa de mensagem
    if resposta == 1:
        #limpa tudo
        for record in minha_arvore.get_children():
            minha_arvore.delete(record)

        #Criando a base de dados ou se conectando a uma base existente
        conn = sqlite3.connect('farm_inic.db')
        #Criando uma instância do cursor
        c = conn.cursor()

        #remover a base de dados inteira
        c.execute("DROP TABLE tabela_medicamentos")

        #Salvar alterações
        conn.commit()
        #Fechar a conexão
        conn.close()
        #limpa as entradas
        limpa_entradas()
        #recriar a tabela
        cria_base_dados_novamente()

#limpa caixas de entrada
def limpa_entradas():
    txtbox_nome.delete(0, END)
    txtbox_fabricante.delete(0, END)
    txtbox_ID.delete(0, END)
    txtbox_substancia.delete(0, END)
    txtbox_lote.delete(0, END)
    txtbox_secao.delete(0, END)
    txtbox_preco.delete(0, END)

#Selecionando registros
def seleciona_reg(e):
    #limpa entrada de registro
    txtbox_nome.delete(0, END)
    txtbox_fabricante.delete(0, END)
    txtbox_ID.delete(0, END)
    txtbox_substancia.delete(0, END)
    txtbox_lote.delete(0, END)
    txtbox_secao.delete(0, END)
    txtbox_preco.delete(0, END)
    #agarrar número de registro
    selected = minha_arvore.focus()
    #agarrar valores de registro
    values = minha_arvore.item(selected, 'values')
    #saídas para as entradas de usuário
    txtbox_nome.insert(0, values[0])
    txtbox_fabricante.insert(0, values[1])
    txtbox_ID.insert(0, values[2])
    txtbox_substancia.insert(0, values[3])
    txtbox_lote.insert(0, values[4])
    txtbox_secao.insert(0, values[5])
    txtbox_preco.insert(0, values[6])  

#atualizar registros
def atualiza_registro():
    #agarrar o número do registro
    selected = minha_arvore.focus()
    #atualiza registro
    minha_arvore.item(selected, text="", values=(txtbox_nome.get(), txtbox_fabricante.get(), txtbox_ID.get(), txtbox_substancia.get(), txtbox_lote.get(), txtbox_secao.get(), txtbox_preco.get(),))

    #atualiza a base de dados
    #Criando a base de dados ou se conectando a uma base existente
    conn = sqlite3.connect('farm_inic.db')
    #Criando uma instância do cursor
    c = conn.cursor() #"oid" é a chave primária
    c.execute("""UPDATE tabela_medicamentos SET
        nome = :nome,
        fabricante = :fabricante,
        substancia = :substancia,
        lote = :lote,
        secao = :secao,
        preco = :preco

        WHERE oid = :oid""",
        {
            'nome': txtbox_nome.get(),
            'fabricante': txtbox_fabricante.get(),
            'substancia': txtbox_substancia.get(),
            'lote': txtbox_lote.get(),
            'secao': txtbox_secao.get(),
            'preco': txtbox_preco.get(),
            'oid': txtbox_ID.get()
        }
        )
    
    #Salvar alterações
    conn.commit()
    #Fechar a conexão
    conn.close()

    #limpa entradas de registro
    txtbox_nome.delete(0, END)
    txtbox_fabricante.delete(0, END)
    txtbox_ID.delete(0, END)
    txtbox_substancia.delete(0, END)
    txtbox_lote.delete(0, END)
    txtbox_secao.delete(0, END)
    txtbox_preco.delete(0, END)

#adicionando registros à base de dados
def adiciona_reg():
    #Criando a base de dados ou se conectando a uma base existente
    conn = sqlite3.connect('farm_inic.db')
    #Criando uma instância do cursor
    c = conn.cursor()
    #adicionando novo registro
    c.execute("INSERT INTO tabela_medicamentos VALUES (:nome, :fabricante, :id, :substancia, :lote, :secao, :preco)",
        {
            'nome': txtbox_nome.get(),
            'fabricante': txtbox_fabricante.get(),
            'id': txtbox_ID.get(),
            'substancia': txtbox_substancia.get(),
            'lote': txtbox_lote.get(),
            'secao': txtbox_secao.get(),
            'preco': txtbox_preco.get(),
        })

    #Salvar alterações
    conn.commit()
    #Fechar a conexão
    conn.close()

    #limpa entradas de registro
    txtbox_nome.delete(0, END)
    txtbox_fabricante.delete(0, END)
    txtbox_ID.delete(0, END)
    txtbox_substancia.delete(0, END)
    txtbox_lote.delete(0, END)
    txtbox_secao.delete(0, END)
    txtbox_preco.delete(0, END)

    #limpar a tabela na treeview
    minha_arvore.delete(*minha_arvore.get_children())
    #mostrar novamente a base de dados na treeview
    consulta_base_dados()

def cria_base_dados_novamente():
    #Criando a base de dados ou se conectando a uma base existente
    conn = sqlite3.connect('farm_inic.db')
    #Criando uma instância do cursor
    c = conn.cursor()

    #Criar tabela
    c.execute("""CREATE TABLE if not exists tabela_medicamentos (
        nome text,
        fabricante text,
        id integer,
        substancia text,
        lote text,
        secao text,
        preco text)
    """)

    """#Adicionar base de dados fictícia à tabela
    for record in dados:
        c.execute("INSERT INTO tabela_medicamentos VALUES (:nome, :fabricante, :id, :substancia, :lote, :secao, :preco)",
            {
                'nome': record[0],
                'fabricante': record[1],
                'id': record[2],
                'substancia': record[3],
                'lote': record[4],
                'secao': record[5],
                'preco': record[6]
            })
    """

    #Salvar alterações
    conn.commit()
    #Fechar a conexão
    conn.close()

def habilita_estoque():
    frm_botoes.place(x=340, y=470)
    dados_frame.place(x=260, y=116)
    tree_frame.place(x=260, y=200)

def fecha_janela():
    janela_inicial.destroy()

#CABEÇALHO
frame_titulo = Frame(janela_inicial, width=10, height=1, bd=1, bg="white")
frame_titulo.place(x=380, y=1)
lbl_titulo = Label(frame_titulo, bg="white", image=logo_farma)
lbl_titulo.grid(row=0, columnspan=2)

#Adicionando botões
frm_botoes = LabelFrame(janela_inicial, text="Comandos", width=10)


btn_atualiza = Button(frm_botoes, text="Atualizar", command=atualiza_registro)
btn_atualiza.grid(row=0, column=0, padx=10, pady=10)

btn_adiciona = Button(frm_botoes, text="Adicionar", command=adiciona_reg)
btn_adiciona.grid(row=0, column=1, padx=10, pady=10)

btn_remover_tudo = Button(frm_botoes, text="Remover todos", command=remove_todos)
btn_remover_tudo.grid(row=0, column=2, padx=10, pady=10)

btn_remover_um = Button(frm_botoes, text="Remover um", command=remove_um)
btn_remover_um.grid(row=0, column=3, padx=10, pady=10)

btn_remover_varios = Button(frm_botoes, text="Remover vários", command=remove_varios)
btn_remover_varios.grid(row=0, column=4, padx=10, pady=10)

btn_mover_acima = Button(frm_botoes, text="Mover acima", command=move_acima)
btn_mover_acima.grid(row=1, column=0, padx=10, pady=10)

btn_mover_abaixo = Button(frm_botoes, text="Mover abaixo", command=move_abaixo)
btn_mover_abaixo.grid(row=1, column=1, padx=10, pady=10)

btn_seleciona_registro = Button(frm_botoes, text="Limpa entradas", command=limpa_entradas)
btn_seleciona_registro.grid(row=1, column=2, padx=10, pady=10)

#BOTÕES LATERAIS
frame_botoes = Frame(janela_inicial, bg="white", bd=3)
frame_botoes.place(x=30, y=100)
lbl_botoes1 = Label(frame_botoes, bg="white", bd=10)
lbl_botoes1.grid(row=0, column=0)
btn_vendas = Button(lbl_botoes1, bg="white", bd=3, image=icone_vendas)
btn_vendas.grid(row=0, column=0)
#--
lbl_botoes2 = Label(frame_botoes, bg="white", bd=10)
lbl_botoes2.grid(row=0, column=1)
btn_estoque = Button(lbl_botoes2, bg="white", bd=3, image=icone_estoque, command=habilita_estoque)
btn_estoque.grid(row=0, column=2)
#--
lbl_botoes3 = Label(frame_botoes, bg="white", bd=10)
lbl_botoes3.grid(row=1, column=0)
btn_financeiro = Button(lbl_botoes3, bg="white", bd=3, image=icone_financeiro)
btn_financeiro.grid(row=1, column=0)
#--
lbl_botoes4 = Label(frame_botoes, bg="white", bd=10)
lbl_botoes4.grid(row=1, column=1)
btn_administracao = Button(lbl_botoes4, bg="white", bd=3, image=icone_funcionarios)
btn_administracao.grid(row=1, column=2)

#ÁREA DO SUPORTE
frame_suporte = Frame(janela_inicial)
frame_suporte.place(x=100, y=350)
lbl_sair = Label(frame_suporte, bg="white", bd=1, anchor=N)
lbl_sair.grid()
btn_sair = Button(lbl_sair, bd=3, bg="white", image=icone_suporte)
btn_sair.grid(row=0, column=1)

#BOTÃO SAIR
#frame_sair = Frame(janela_inicial)
#frame_sair.grid(row=4, columnspan=2, sticky=N)
lbl_sair = Label(frame_suporte, bg="white", bd=12)
lbl_sair.grid()
btn_sair = Button(lbl_sair, bd=3, bg="white", image=icone_desliga, command=fecha_janela)
btn_sair.grid(row=0, column=1)

#ASSINATURA
frame_assinatura = Frame(janela_inicial)
frame_assinatura.place(x=300, y=580)
lbl_assinatura = Label(frame_assinatura, text="Desenvolvido por Marco Moraes", font="Arial 14 italic", bg="white", bd=30)
lbl_assinatura.grid()

#vincular a treeview
minha_arvore.bind("<ButtonRelease-1>", seleciona_reg)

#Executar para colocar dados da base de dados ao iniciar
consulta_base_dados()

janela_inicial.mainloop()