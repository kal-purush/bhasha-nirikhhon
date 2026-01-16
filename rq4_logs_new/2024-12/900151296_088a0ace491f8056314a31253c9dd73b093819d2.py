import random
import os

def Jogo(Word):
    Tam = len(Word)
    Tentativas = random.randint(6,11)
    Caracter = []
    Layout = []
    Verificador = []
    Acertos = 0
    Erros = ''

    for i in range(Tam):
        if Word[i] == ' ':
            Layout.append(' ')
            Caracter.append(Word[i])
            Acertos += 1
        else:
            Caracter.append(Word[i])
            Layout.append("_ ")

     
    while Acertos != Tam and Tentativas != 0:
        os.system('cls')
        StringLayout = ' '.join(Layout)
        print(f'* Forca *\nDica: Personagem da Marvel e DC\nErros: {Erros}\nTentativas restantes: {Tentativas}\n\n\n{StringLayout}')
            
        Resp = str(input('\nDigite uma letra: ')).upper()
        while len(Resp) != 1:
            Resp = str(input('\nDigite uma letra: ')).upper()

        if Resp not in Verificador:
            for i in range(Tam):
                if Resp == Caracter[i]:
                    Layout[i] = f'{Caracter[i]} '
                    Acertos += 1
                    Verificador.append(Resp)

        if Resp not in Caracter:
            Erros += f'{Resp} '
            Tentativas -= 1  


    os.system('cls')
    if Tentativas == 0:
        print(f'* Fim das tentativas *\nResposta: {Word}\nFim de Jogo')
    else:
        StringLayout = ' '.join(Layout)
        print(f'\nVocê conseguiu!\n* Forca *\n{StringLayout}\nFim de Jogo!')



Base = []
Cont = -1
with open("Forca.txt",'r', encoding='utf-8') as arquivo:
    for linha in arquivo:
        linha = linha.strip()
        Base.append(linha)
        Cont += 1

while True:
    i = random.randint(0,len(Base))
    Word = Base[i].upper()
    Jogo(Word)

    Continuar = int(input('\n* Deseja continuar *\n1- Sim\n2- Não\nR: '))
    while Continuar != 1 and Continuar != 2:
        Continuar = int(input('* Deseja continuar *\n1- Sim\n2- Não\nR: '))
    
    if Continuar == 2:
        print('Programa Encerrado!')
        break