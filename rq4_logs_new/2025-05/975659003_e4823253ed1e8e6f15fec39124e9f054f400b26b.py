import sqlite3

conexao = sqlite3.connect("C:\jess\Projetos-em-conjunto")
cursor = conexao.cursor()

cursor.execute('''
               create table if not exists setor (
               setor_id integer primary key,
               nome_setor text not null,
               nome_responsavel text not null
               )
''')

cursor.execute('''
               create table if not exists cargo (
               cargo_id integer primary key,
               nome_cargo text not null,
               tipo_contrato text check(tipo_contrato in('pj', 'clt')),
               setor_id integer,
               foreign key (setor_id) references setor(setor_id)
               )
''')

cursor.execute('''
               create table if not exists funcionario (
               funcionario_id integer primary key,
               cargo_id integer not null, 
               nome_funcionario text not null,
               salario integer not null,
               data_admissao date,
               fim_contrato date,
               senha text,
               foreign key (cargo_id) references cargo(cargo_id)
               )
''')

cursor.execute('''
               create table if not exists produto (
               produto_id integer primary key,
               nome_produto text not null,
               tipo_produto text not null
               )
''')


cursor.execute('''
               create table if not exists estoque (
               produto_id integer primary key,
               qntd_produto integer not null,
               validade date,
               foreign key (produto_id) references produto(produto_id))
               ''')


cursor.execute('''
               create table if not exists cliente (
               cliente_cpf text primary key,
               telefone1 integer not null, 
               telefone2 integer,
               email text not null
               )
''')


cursor.execute('''
               create table if not exists enderecos (
               cliente_cpf text primary key,
               cidade text not null,
               bairro text not null,
               rua text not null,
               numero integer not null,
               numero_andar null,
               foreign key (cliente_cpf) references cliente(cliente_cpf)
               )
''')

cursor.execute('''
               create table if not exists compra (
               compra_id integer primary key,
               produto_id,
               qntd_produto integer,
               foreign key(produto_id) references produto(produto_id))
''')

cursor.execute('''
               create table if not exists venda (
               venda_id integer primary key,
               funcionario_id integer,
               cliente_cpf text,
               compra_id integer,
               foreign key (funcionario_id) references funcionario(funcionario_id),
               foreign key (cliente_cpf) references cliente(cliente_cpf),
               foreign key (compra_id) references compra(compra_id)
               )
''')

cursor.execute('''
               create table if not exists pagamento_loja (
               compra_id integer primary key,
               tipo_pagamento text not null, 
               data_transferencia date,
               foreign key (compra_id) references compra(compra_id))
               ''')


cursor.execute('''
               create table if not exists encomenda (
               encomenda_id integer primary key,
               cliente_cpf text, 
               produto_id integer,
               qntd_produto integer,
               data_pedido date,
               data_entrega date,
               foreign key (cliente_cpf) references cliente(cliente_cpf),
               foreign key (produto_id) references produto(produto_id))
               ''')

cursor.execute('''
               create table if not exists pagamento_encomenda (
               encomenda_id integer primary key,
               tipo_pagamento text not null, 
               data_transferencia date,
               foreign key (encomenda_id) references encomenda(encomenda_id))
               ''')

conexao.commit()
conexao.close()