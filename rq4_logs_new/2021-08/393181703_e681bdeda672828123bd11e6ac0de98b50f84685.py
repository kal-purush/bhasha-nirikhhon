print('--- CONVERSOR PARA XML ---')


idChangeSet = int(input('Id do changeSet: '))
changeSetInicio = f'<changeSet author="pacto" id="{idChangeSet}">'


fim = True
listaComParametros = []

nomeDaTabela = str(input('Nome da tabela: '))
nomeDaTabela.append(nomeDaTabela)

while fim:
    lista = []

    
    
    for a in range(4):
        nomeDaColuna = str(input(f'Nome da coluna {a}: '))
        valorDaColuna = str(input(f'Valor da coluna {a}: '))

        if a == 4:
            nomeDaColuna = str(input(f'Nome da coluna {a}: '))
            valorDaColuna = str(input(f'Valor da data da coluna {a}: '))

        lista.append(nomeDaColuna)
        lista.append(valorDaColuna)

    listaComParametros.append(lista)

    sair = str(input('Sair do programa [s/n]: ')).strip().lower()[0]
    if sair == 's':
        break

for a in range(len(listaComParametros)):
    for b in range(len(listaComParametros[0])):
        coluna0 = f'<column name={listaComParametros[a][b]} value={listaComParametros[a][b]}/>'
        coluna1 = f'<column name="tivo" value="S"/>'
        <column name="usuario_cadastro" value="00000000000"/>
        <column name="data_cadastro" valueDate={listaComParametros[a][b]}/>


inicio = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>'
inicio2 = '<databaseChangeLog xmlns="http://www.liquibase.org/xml/ns/dbchangelog" xmlns:ext="http://www.liquibase.org/xml/ns/dbchangelog-ext" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog-ext http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-ext.xsd http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.1.xsd">'
propriedade = '<property name="now" value="now()" dbms="mysql"/>'



print('Lista: ', listaComParametros)