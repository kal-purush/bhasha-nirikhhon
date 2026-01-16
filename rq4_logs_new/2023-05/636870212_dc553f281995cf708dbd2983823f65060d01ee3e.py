import menu 

global nome #nome do local (alterar depois)
    

def ver_perfil():
    try:
        with open('local.csv', 'r', encoding='utf8') as f:
            linhas = f.readlines()

            for linha in linhas:
                dados = linha.strip().split(';')
                if nome == dados[0]:
                    print(f'Nome: {dados[0]}; Tipo: {dados[1]}; Endereço: {dados[2]}; Tipo musical: {dados[3]}; Contato: {dados[4]}')

                else:
                    print('Perfil não encontrado')
    except:
        print('ERRO DO SISTEMA')
        return menu #voltar para o menu


def adicionar_informacoes(): #adicionar informações do próprio perfil
    try:
        with open('local.csv', 'a', encoding='utf8') as f:
            tipo = input('Digite o tipo do local: ')
            endereco = input('Digite o endereço do local: ')
            tipo_musical = input('Digite o tipo musical de preferência do local: ')
            contato = input('Digite as informações de contato: ')

            f.write(f'{nome};{tipo};{endereco};{tipo_musical};{contato}\n')
    except:
        print('ERRO DO SISTEMA')
        return menu #voltar para o menu

def editar_perfil():
    try:
        encontrado = False
        with open('local.csv', 'r', encoding='utf8') as f, open('local_temp.csv', 'w', encoding='utf8') as f_temp:
            linhas = f.readlines()

            for linha in linhas:
                dados = linha.strip().split(';')
                if nome == dados[0]:
                    encontrado = True
                    
                    print(f'Nome: {dados[0]}; Tipo: {dados[1]}; Endereço: {dados[2]}; Tipo musical: {dados[3]}; Contato: {dados[4]}')
                    decisao = input('Qual informação deseja editar: ').lower()

                    if decisao == 'nome':
                        novo_nome = input('Digite o novo nome do local (ou deixe em branco para apagar): ')
                        f_temp.write(f'{novo_nome};{dados[1]};{dados[2]};{dados[3]};{dados[4]}\n')
                    
                    elif decisao == 'tipo':
                        novo_tipo = input('Digite o novo tipo do local (ou deixe em branco para apagar): ')
                        f_temp.write(f'{dados[0]};{novo_tipo};{dados[2]};{dados[3]};{dados[4]}\n')
                    
                    elif decisao == 'endereço':
                        novo_endereco = input('Digite o novo endereço do local (ou deixe em branco para apagar): ')
                        f_temp.write(f'{dados[0]};{dados[1]};{novo_endereco};{dados[3]};{dados[4]}\n')
                    
                    elif decisao == 'tipo musical':
                        novo_musica = input('Digite o novo tipo musical de preferência (ou deixe em branco para apagar): ')
                        f_temp.write(f'{dados[0]};{dados[1]};{dados[2]};{novo_musica};{dados[4]}\n')
                    
                    elif decisao == 'contato':
                        novo_contato = input('Digite os novos dados de contato (ou deixe em branco para apagar): ')
                        f_temp.write(f'{dados[0]};{dados[1]};{dados[2]};{dados[3]};{novo_contato}\n')
                    
                    else:
                        print('Opção inválida!')
                
                else:
                    f_temp.write(f'{linha}\n')

        
        if encontrado:
            with open('local.csv', 'w', encoding='utf8') as f, open('local_temp.csv', 'r', encoding='utf8') as f_temp:
                f.write(f'{f_temp.read()}\n')

            with open('local_temp.csv', 'w') as f:
                pass
        
            print('Informações atualizadas com sucesso!')
        
        else:
            with open('local_temp.csv', 'w') as f:
                pass

            print('Perfil do local não encontrado')
    except:
        print('ERRO DO SISTEMA')
        return menu #voltar para o menu


def ver_locais(): #ver todos os locais
    try:
        with open('local.csv', 'r', encoding='utf8') as f:
            linhas = f.readlines()

            for linha in linhas:
                dados = linha.strip().split(';')

                print(f'Nome: {dados[0]}; Tipo: {dados[1]}; Endereço: {dados[2]}; Tipo musical: {dados[3]}; Contato: {dados[4]}')
    except:
        print('ERRO DO SISTEMA')
        return menu #voltar para o menu

def ver_bandas(): #ver todas as bandas
    try:
        with open('banda.csv', 'r', encoding='utf8') as f:
            linhas = f.readlines()

            for linha in linhas:
                dados = linha.strip().split(';')

                print(f'Nome: {dados[0]}; Tipo: {dados[1]}; Integrantes: {dados[2]}; Tipo musical: {dados[3]}; Contato: {dados[4]}')
    except:
        print('ERRO DO SISTEMA')
        return menu #voltar para o menu
    

def ver_agenda():
    try:
        with open('agenda.csv', 'r', encoding='utf8') as f:
            linhas = f.readlines()

            for linha in linhas:
                dados = linha.strip().split(';')

                print(f'Banda: {dados[0]}; Local: {dados[1]}; Data: {dados[2]}; Horário de início: {dados[3]}; Horário de encerramento: {dados[4]}')
    except:
        print('ERRO DO SISTEMA')
        return menu #voltar para o menu

def adicionar_agenda(): #adicionar um show na agenda
    try:
        with open('agenda.csv', 'a', encoding='utf8') as f:
            banda = input('Digite o nome da banda: ')
            data = input('Digite a data: ')
            hora_i = input('Digite o horário de início: ')
            hora_f = input('Digite o horário de encerramento: ')

            f.write(f'{banda};{nome};{data};{hora_i};{hora_f}\n')
    except:
        print('ERRO DO SISTEMA')
        return menu #voltar para o menu


def editar_agenda():
    try:
        encontrado = False
        with open('agenda.csv', 'r', encoding='utf8') as f, open('agenda_temp.csv', 'w', encoding='utf8') as f_temp:
            linhas = f.readlines()
            
            for linha in linhas:
                dados = linha.strip().split(';')

                if nome == dados[1]:
                    encontrado == True

                    print(f'Banda: {dados[0]}; Local: {dados[1]}; Data: {dados[2]}; Horário de início: {dados[3]}; Horário de encerramento: {dados[4]}')
                    decisao = input('Qual informação deseja editar: ').lower()

                    if decisao == 'banda':
                        nova_banda = input('Digite o novo nome da banda (ou deixe em branco para apagar): ')
                        f_temp.write(f'{nova_banda};{dados[1]};{dados[2]};{dados[3]};{dados[4]}\n')
                    
                    elif decisao == 'local':
                        novo_local = input('Digite o novo local (ou deixe em branco para apagar): ')
                        f_temp.write(f'{dados[0]};{novo_local};{dados[2]};{dados[3]};{dados[4]}\n')
                    
                    elif decisao == 'data':
                        nova_data = input('Digite a nova data (ou deixe em branco para apagar): ')
                        f_temp.write(f'{dados[0]};{dados[1]};{nova_data};{dados[3]};{dados[4]}\n')
                    
                    elif decisao == 'tipo musical':
                        novo_hora_i = input('Digite o novo horário de início (ou deixe em branco para apagar): ')
                        f_temp.write(f'{dados[0]};{dados[1]};{dados[2]};{novo_hora_i};{dados[4]}\n')
                    
                    elif decisao == 'contato':
                        novo_hora_f = input('Digite o novo horário de encerramento (ou deixe em branco para apagar): ')
                        f_temp.write(f'{dados[0]};{dados[1]};{dados[2]};{dados[3]};{novo_hora_f}\n')
                    
                    else:
                        print('Opção inválida!')
                
                else:
                    f_temp.write(f'{linha}\n')

            if encontrado:
                with open('agenda.csv', 'w', encoding='utf8') as f, open('agenda_temp.csv', 'r', encoding='utf8') as f_temp:
                    f.write(f'{f_temp.read()}\n')

                with open('local_temp.csv', 'w') as f:
                    pass
            
                print('Agenda atualizada com sucesso!')
            
            else:
                with open('local_temp.csv', 'w') as f:
                    pass

                print('Nenhum show da agenda no seu local encontrado!')

    except:
        print('ERRO DO SISTEMA')
        return menu #voltar para o menu
    