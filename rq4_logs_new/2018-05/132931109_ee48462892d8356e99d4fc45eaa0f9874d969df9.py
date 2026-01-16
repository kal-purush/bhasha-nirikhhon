def convite(conv):
    tamanho = len(conv)
    fim = len(conv)
    ini = fim - 4
    part2 = conv[ini:fim]
    part1 = conv[0:4]
    return ('o codigo gerado foi: %s %s' %(part1.lower().capitalize(),
                                           part2.lower().capitalize()))


nomes = []

def cadastrar(nomes):
    nome = input('nome: ')
    nomes.append(nome)

def listar(nomes):
    print('listar nomes:')
    for nome in nomes:
        print(nome.capitalize())

def menu():
    nomes = []
    opcao = ''
    while opcao != 0:
        opcao = input('\n1 - cadastrar\n2 - listar nomes\n0 - sair\n')
        
        if(opcao == '1'):
            cadastrar(nomes)
        if(opcao == '2'):
            listar(nomes)
        elif(opcao == '0'):
            return ('Fim')
menu()

def gera(nomes):
    posicao_final = len(nomes)
    posicao_inicial = posicao_final - 4
    parte1 = nomes[0:4]
    parte2 = nomes[posicao_inicial :posicao_final]
    print ("%s %s" %(parte1, parte2))
    return ('Enviando convite para %s ' % (nomes))

def procurar_regex(nomes):
    print('Digite a expressão regular:')
    regex = input()
    nomes_concatenados = ' '.join(nomes)
    resultados = re.findall(regex, nomes_concatenados)
    print(resultados) 