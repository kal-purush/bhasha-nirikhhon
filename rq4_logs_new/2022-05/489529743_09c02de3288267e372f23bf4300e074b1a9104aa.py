from copy import copy
from queue import Queue
from itertools import groupby
import time



#Criação da classe Nó
class No:
#Dentro da classe, é criado a funcão _init_, que guardará as propriedades desse Nó
    def __init__(self, estado, custo_caminho, custo_busca, caminho):
        self.estado = estado                #estado do no, ou seja, sua posição atual
        self.custo_caminho = custo_caminho  #custo do caminho do Nó
        self.custo_busca = custo_busca      #custo da busca
        self.caminho = copy(caminho)        #caminho do Nó

#Função para trocar posições na lista
def troca(list, pos1, pos2):
    list[pos1], list[pos2] = list[pos2], list[pos1]
    return list

def imprime_estado(estado):
    print('        -------------------------')
    print('        |       |       |       |')
    print(f'        |   {estado[0]}   |   {estado[1]}   |   {estado[2]}   |')
    print('        |       |       |       |')
    print('        -------------------------')
    print('        |       |       |       |')
    print(f'        |   {estado[3]}   |   {estado[4]}   |   {estado[5]}   |')
    print('        |       |       |       |')
    print('        -------------------------')
    print('        |       |       |       |')
    print(f'        |   {estado[6]}   |   {estado[7]}   |   {estado[8]}   |')
    print('        |       |       |       |')
    print('        -------------------------')

#Aplica as ações possíveis ao Nó, gerando os 4 filhos 
def expande(no):
    #Guarda a posição do 9
    pos_nove = no.estado.index('9')

    #Definição do tamanho do quadrado
    comprimento = 3
    altura = 3
    tam_quadrado = comprimento * altura

    #Declaração das posições como constantes
    CIMA = -comprimento
    BAIXO = comprimento
    ESQUERDA = -1
    DIREITA = 1

    #Criação da lista de nós expandidos
    nos_expandidos = []

    #Move pra cima, se possível
    if (pos_nove + CIMA >= 0):
        #Cria um novo estado pra guardar o estado atual
        novo_estado = copy(no.estado)
        #Troca as posições do 9 com o Nº de Cima
        troca(novo_estado, pos_nove, pos_nove + CIMA)
        #Cria um novo caminho pra guardar o caminho atual e adiciona o novo estado 
        novo_caminho = copy(no.caminho)
        novo_caminho.append(novo_estado)
        #Cria um novo Nó com o estado atual
        novo_no = No(novo_estado, no.custo_caminho + 1, -1, novo_caminho)
        #Adiciona o novo nó no conjunto de nós expandidos
        nos_expandidos.append(novo_no)

    #Move pra esquerda, se possível
    if ((pos_nove + ESQUERDA) % comprimento != comprimento - 1):
        #Cria um novo estado pra guardar o estado atual
        novo_estado = copy(no.estado)
        #Troca as posições do 9 com o Nº de Esquerda
        troca(novo_estado, pos_nove, pos_nove + ESQUERDA)
        #Cria um novo caminho pra guardar o caminho atual e adiciona o novo estado 
        novo_caminho = copy(no.caminho)
        novo_caminho.append(novo_estado)
        #Cria um novo Nó com o estado atual
        novo_no = No(novo_estado, no.custo_caminho + 1, -1, novo_caminho)
        #Adiciona o novo nó no conjunto de nós expandidos
        nos_expandidos.append(novo_no)

    #Move pra baixo, se possível
    if (pos_nove + BAIXO < tam_quadrado):
        #Cria um novo estado pra guardar o estado atual
        novo_estado = copy(no.estado)
        #Troca as posições do 9 com o Nº de Baixo
        troca(novo_estado, pos_nove, pos_nove + BAIXO)
        #Cria um novo caminho pra guardar o caminho atual e adiciona o novo estado 
        novo_caminho = copy(no.caminho)
        novo_caminho.append(novo_estado)
        #Cria um novo Nó com o estado atual
        novo_no = No(novo_estado, no.custo_caminho + 1, -1, novo_caminho)
        #Adiciona o novo nó no conjunto de nós expandidos
        nos_expandidos.append(novo_no)

    #Move pra direita, se possível
    if ((pos_nove + DIREITA) % comprimento != 0):
        #Cria um novo estado pra guardar o estado atual
        novo_estado = copy(no.estado)
        #Troca as posições do 9 com o Nº de Direita
        troca(novo_estado, pos_nove, pos_nove + DIREITA)
        #Cria um novo caminho pra guardar o caminho atual e adiciona o novo estado 
        novo_caminho = copy(no.caminho)
        novo_caminho.append(novo_estado)
        #Cria um novo Nó com o estado atual
        novo_no = No(novo_estado, no.custo_caminho + 1, -1, novo_caminho)
        #Adiciona o novo nó no conjunto de nós expandidos
        nos_expandidos.append(novo_no)
    #Retorna a lista de nós expandidos
    return nos_expandidos

#Função para verificar se é mágico
def quad_mag(lista): 
    lista = list(map(int, lista))#transformando os valores da lista em int
    sl1= [lista[0],lista[1],lista[2]]#Soma da linha 1
    sl2= [lista[3],lista[4],lista[5]]#Soma da linha 2
    sl3= [lista[6],lista[7],lista[8]]#Soma da linha 3
    sc1 = [sl1[0],sl2[0],sl3[0]]#Soma da coluna 1
    sc2 = [sl1[1],sl2[1],sl3[1]]#Soma da coluna 2
    sc3 = [sl1[2],sl2[2],sl3[2]]#Soma da coluna 2        
    sd1 = [sl1[0],sl2[1],sl3[2]]#Soma da diagonal 1
    sd2 = [sl1[2],sl2[1],sl3[0]]#Soma da diagonal 2
    somas = [sum(sl1),sum(sl2),sum(sl3),sum(sc1),sum(sc2),sum(sc3),sum(sd1),sum(sd2)]#lista de soma dos valores
    if somas[0] == 15:
        return td_igual(somas)#retorna um booleano se todos os valores de somas são iguais
    else:
        return None
#Função pra verificar se todos os valores de uma lista são iguais
def td_igual(lista):
    g = groupby(lista)  #função pra retornar chave e iterável da lista
    return next(g, True) and not next(g, False) #retorna o booleano se são iguais ou não

#Função da busca em largura a partir de um estado inicial
def busca_em_largura(estado_inicial):
    #Define o nó inicial como o estado inicial
    no_inicial = No(estado_inicial, 0, 1, [estado_inicial])
    #Verifica se o nó inicial é o objetivo(quadrado mágico com somas das linhas, colunas e digonais iguais)
    if quad_mag(no_inicial.estado):
        #Retorna o nó inicial como objetivo
        return no_inicial
    #Configura a fila de prioridade com o nó inicial no começo dela
    fila = Queue()
    fila.put(no_inicial)
    #Cria um conjunto dos estados percorridos e adiciona o estado inicial nele
    estados_percorridos = set()
    estados_percorridos.add(''.join(no_inicial.estado))
    #Define o custa de cada ação
    custo_busca = 1

    #Enquanto tiver itens na fila ele executa esse while
    while not fila.empty():
        #Pega o nó atual no primeiro lugar da fila
        no_atual = fila.get()

        #Para cada estado possível no Nó atual ele vai verificar se é o objetivo e adicionar nos estados percorridos
        for no_expandido in expande(no_atual):
            custo_busca += 1
            #Verificação do objetivo
            if quad_mag(no_expandido.estado):
                no_expandido.custo_busca = custo_busca
                return no_expandido
            #Adicionando estado aos estados percorridos
            if ''.join(no_expandido.estado) not in estados_percorridos:
                fila.put(no_expandido)
                estados_percorridos.add(''.join(no_expandido.estado))
    #Não retorna itens se não for o objetivo
    return None

def busca_em_profundidade(estado_inicial):
    #Cria uma pilha pra armazenar os estados
    pilha = []
    pilha.append([estado_inicial, 0]) #[estado, nivel]
    #Definição das variaveis usada na busca em profundidade
    custo_busca = 0
    caminho = [] #lista do caminho de estados
    alcancados = set() #Conjunto de estados alcançados 
    solucionado = False #Se já foi solucionado
    nivel = -1 #Nível da árvore

    #Rodar o loop no tamanho da pilha
    while len(pilha) != 0:
        #O topo recebe o primeiro item da pilha
        topo = pilha.pop()
        #E o estado atual vai ser o primeiro do topo
        estado_atual = topo[0]

        nivel_anterior = nivel #Nivel anterior pras buscas
        nivel = topo[1] #Nível do estado na arvore

        #Verifica se o nível atual é menor que o nivel anterior pra tirar do caminho
        if nivel < nivel_anterior:
            caminho.pop()
        #Verifica se o estado atual está no conjunto dos alcançados
        if ''.join(estado_atual) not in alcancados:
            custo_busca += 1 #Adiciona ao custo da busca
            caminho.append(estado_atual) #E adiciona o estado ao caminho
            #Verifica se achou o estado objetivo
            if quad_mag(estado_atual):
                solucionado = True
                break
            #Adiciona o estado atual nos alcançados 
            alcancados.add(''.join(estado_atual))
            #Pega a posição do 9 na lista
            pos_nove = estado_atual.index('9') 
            #Definição do tamanho do quadrado
            comprimento = 3
            altura = 3
            tam_quadrado = comprimento * altura
            CIMA = -comprimento
            BAIXO = comprimento
            ESQUERDA = -1
            DIREITA = 1
            nivel_novo = nivel + 1

            #Move pra cima, se possível
            if (pos_nove + CIMA >= 0):
                #Cria um novo estado 
                novo_estado = copy(estado_atual) #Cria um novo estado pra guardar o estado atual
                troca(novo_estado, pos_nove, pos_nove + CIMA)#Troca as posições do 9 com o Nº de Cima
                pilha.append([novo_estado, nivel_novo])

            #Move pra esquerda, se possível
            if ((pos_nove + ESQUERDA) % comprimento != comprimento - 1):
                novo_estado = copy(estado_atual)#Cria um novo estado pra guardar o estado atual
                troca(novo_estado, pos_nove, pos_nove + ESQUERDA)#Troca as posições do 9 com o Nº de Esquerda
                pilha.append([novo_estado, nivel_novo])

            #Move pra baixo, se possível
            if (pos_nove + BAIXO < tam_quadrado):
                novo_estado = copy(estado_atual)#Cria um novo estado pra guardar o estado atual
                troca(novo_estado, pos_nove, pos_nove + BAIXO)#Troca as posições do 9 com o Nº de Baixo
                pilha.append([novo_estado, nivel_novo])

            #Move pra direita, se possível
            if ((pos_nove + DIREITA) % comprimento != 0):
                novo_estado = copy(estado_atual)#Cria um novo estado pra guardar o estado atual
                troca(novo_estado, pos_nove, pos_nove + DIREITA)#Troca as posições do 9 com o Nº de Direita
                pilha.append([novo_estado, nivel_novo])

    #Verifica se foi solucionado a busca
    if solucionado:
        return No(caminho[-1], len(caminho) - 1, custo_busca, caminho)
    else:
        return None

estado_inicial = '6 9 8 7 1 3 2 5 4'
estado_inicial = estado_inicial.split()
inicio = time.time()
while True:
    op = int(input("Tipo de busca? \n1 - Largura\n2 - Profundidade\n..."))
    if op == 1:
        solucao = busca_em_largura(estado_inicial)
        fim = time.time()
        print(f'\n\n Tempo de execução: {fim - inicio}\n\n')
        break
    elif op == 2:
        solucao = busca_em_profundidade(estado_inicial)
        fim = time.time()
        print(f'\n\n Tempo de execução: {fim - inicio}\n\n')
        break
    else:
        print("Opção invalida!")
        print('\n---------------------------------------------')

#Verificação se foi encontrada a solução
if solucao == None:
    print('\nNão foi encontrada uma solução para essa configuração!\n\nVerifique os valores de entrada e tente novamente.')
else:
    print(f"\nCusto da busca: foram percorridos {solucao.custo_busca} estado(s)")
    print(f"\nCusto da solução: a solução encontrada exige {solucao.custo_caminho} movimento(s)")
    print('\n---------------------------------------------')
    
    while True:
        op = int(input("Deseja ver o passo a passo?\n1 - Sim\n2 - Não\n..."))
        if op == 1:
            input('\nPressione ENTER para ver o passo a passo\n')
            cont = 1
            #Mostra os estados passados até a solução
            for estado in solucao.caminho:
                print(f"\n-----------------ESTADO {cont}------------------\n")
                imprime_estado(estado)
                print('\n---------------------------------------------')
                input('\nPressione ENTER para avançar\n')
                cont += 1
            print("\n-------------------FIM!---------------------\n")
            imprime_estado(estado)
            break
        elif op == 2:
            print("\n-------------------FIM!---------------------\n")
            imprime_estado(solucao.estado)
            break
        else:
            print('\n---------------------------------------------')
            print("Opção invalida!")
print('\n---------------------------------------------')
input('\nPressione ENTER para sair\n')