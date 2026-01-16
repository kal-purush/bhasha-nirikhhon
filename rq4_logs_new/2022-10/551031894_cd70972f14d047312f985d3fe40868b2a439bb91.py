# -*- coding: utf-8 -*-
"""
Created on Wed Aug  4 07:56:10 2021

@author: Fabricio
"""

import random

# board é uma lista de caracteres
board = ['''
             _________
            |        |
            |
            |
            |
            |
          =======''', '''
          
             _________
            |        |
            |        O
            |
            |
            |
          =======''', '''
          
             _________
            |        |
            |        O
            |        | 
            |
            |
          =======''', '''
          
             _________
            |        |
            |        O
            |       /| 
            |
            |
          =======''', '''
          
             _________
            |        |
            |        O
            |       /|\ 
            |
            |
          =======''', '''
          
             _________
            |        |
            |        O
            |       /|\ 
            |       / 
            |
          =======''', '''
          
             _________
            |        |
            |        O
            |       /|\ 
            |       / \ 
            |
          =======''']
          
          
print('########### Jogo da Forca ##########')
print(board[0])

# lista com palavras
palavras = ['massa', 'casa', 'corra','festa', 'pizza', 'carro', 'abacaxi',
            'banana', 'uva', 'toalha', 'feriado', 'trabalho', 'xadrez'] 
palavra = random.choice(palavras) # escolhe uma palavra aleatoriamente
secreta = list(len(palavra)*'_') # faz o tracejado para adivinhar a palavra

#print(palavra)

print('\nPalavra = ',*secreta) # *secreta imprime os elementos da lista
# secreta sem os []

letras_corretas = []
letras_erradas = []

print('Letras Corretas: '.join(letras_corretas)) # join imprime os elementos 
print('Letras erradas: '.join(letras_erradas)) # de uma lista sem []


    
tentativas = 6 # número de chances antes de ser enforcado
i=0 # iterador que imprime a figura da lista board na posição i

while tentativas > 0 and list(palavra) != secreta:
# palavra e secreta no fim do jogo serão iguais, mas secreta é uma lista
# enquanto que palavra uma string, para fazer a comparação é preciso 
# transformar palavra em lista, o qual separa todas as suas letras em uma lista

# A função join retorna os elementos de uma lista separados pelo caracter
#rscolhido (',') sem os colchetes.       
    print('\nLetras Corretas: ', ', '.join(letras_corretas))
    print('Letras erradas: ', ', '.join(letras_erradas))
    chute = input('Digite a letra: ')
    if chute in palavra:
# esse if interior verifica se a letra já foi escolhida antes
        if chute in letras_corretas:
            print('Essa letra já foi escolhida')
            pass
        else:
            letras_corretas.append(chute)
            letras_corretas.sort() # ordena a lista

# a é uma lista de tupla com os indice e a letra da palavra, se o chute for uma
# letra contida em palavra, então trocamos o caracter '_' da lista secreta pela
# letra na posição do indice correspondente. Isso faz funcionar para palavras com
# letras repetidas
            a = list(enumerate(palavra))
            for k, j in a:
                if j==chute:
                    secreta[k]=chute
        print(board[i])
        
        if list(palavra) == secreta:
            print('\nA palavra era {}'.format(palavra))
            print('\n###### Parabéns!! ######')
            print('###### Você Venceu!! #####')
            break
        
        
    else:
        if chute in letras_erradas:
            print('Essa letra já foi escolhida')
            pass
        else:
            letras_erradas.append(chute)
            letras_erradas.sort() # ordena a lista
        
            i += 1
            print(board[i]) # imprime a figura correspondente
            
            tentativas -=1
            if i == len(board) - 1:
                print('\n######## Fim de Jogo #########')
                print('####### Você foi enforcado #####')
    
    print('\nPalavra = ', *secreta)   
    
   
    





