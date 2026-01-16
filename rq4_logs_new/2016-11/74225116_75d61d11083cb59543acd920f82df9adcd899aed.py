# -*- coding: cp1252 -*-
## Penalt in Python 3
from random import randint
##======== contadores para o placar =======
contador1=int(0)
contador2=int(0)
##=========================================
##============== Introdu��o ===========================
print'========= Penalt in Python 3 ================'
print'      ==  A disputa Continua =='
print' Um jogo de disputa de Penalt para ver que � melhor, voc� ou seu amigo!'
##====================================================================================
##==================== Escolha do modo de jogo Individual ou MultiPlayer ===================================================================
x=int(raw_input('\n\tEm qual modo voc� deseja jogar?\n\t 1. Individual (PLAYER vs CPU)\n\t 2. MultiPlayer (PLAYER vs PLAYER)\n\t'))
##=================== A��o para que usuario n�o digite um valor diferente das desejadas(obs.: esta dando erro caso valor errado)
while 1:
   try:
       if x==1 or x==2:
            break
        
   except:
       print'\nVoce digitou um modo inesistente de jogar'
       print'por favor digite novamente'
       x=int(raw_input("\n\tEm qual modo voc� deseja jogar?\n\t 1. Individual (PLAYER vs CPU)\n\t 2. MultiPlayer (PLAYER vs PLAYER)\n\t"))
##============= Fim da a��o para qe usuario n�o digite valor diferente ==============================================       
###================== FIm da escolha de modo de jogo =========================================================================================
##================= INDIVIDUAL ==============================

if (x==1):
    print '\t\n\nBem vindo, nome � Sean e eu vou acabar com voce nesta disputa'
    nome = raw_input('\t\nQual � o seu nome?\n')
    print '\t\nPraser em te conhecer %s, agora vamos ao que intere�a...\n\t ...VAMOS JOGAR!!!!' % nome
while 1:
   print'\n\tEscolha o lugar onde voc� quer chutar'
   print'\n\tPara chutar no meio do gol digite 1.'
   print'\n\tPara chutar ao lado direito do gol digite 2.'
   print'\n\tPara chutar ao lado esquerdo do gol digite 3.'
   
   c=raw_input("\n\n\tOnde voce deseja chutar?\n")
   
   while 1:
      
      try:
         c=int(c)
         break
      except:
         print'Voce digitou um lugar nao existente'
         print'por favor digite novamente'
         c=raw_input("Onde voce deseja chutar?\n")
   d=randint(1,3)
   if(c!=d):
      print'O goleito agarrou seu chute'
   else:
      print'GOOOOOOOOOOOLLL'
      contador1=contador1 + 1
      
   print nome,contador1, 'x', contador2,'Sean'
   
   print'\n\tEscolha o lugar onde voc� quer chutar'
   print'\n\tPara chutar no meio do gol digite 1.'
   print'\n\tPara chutar ao lado direito do gol digite 2.'
   print'\n\tPara chutar ao lado esquerdo do gol digite 3.'

   c=raw_input("\n\nOnde voce deseja defender?\n")
   
   
   while 1:
      try:
         c=int(c)
         break
      except:
         print'Voce digitou um lugar nao existente'
         print'por favor digite novamente'
         c=raw_input("\n\nOnde voce deseja defender?\n")
   d=randint(1,3)
   if(c!=d):
      print'Voce agarrou o chute'
   else:
      print'GOOOOOOOOOOOLLL'
      contador2=contador2 + 1
      
   print nome,contador1, 'x', contador2,'Sean'
   
##===========================================================================================================