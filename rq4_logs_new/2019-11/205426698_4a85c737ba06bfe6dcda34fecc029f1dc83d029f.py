# Curso Python #15 - Interrompendo repetições while

# Exemplo 01:
#   enquanto VERDADEIRO
#       se terra
#         passo
#       se buraco
#           pula
#       se moeda
#          pega
#       se troféu
#           pula
#           interrompa
# pega

# Transformando em Python:
# while True:
#   if terra:
#       passo
#   if buraco:
#       pula
#   if moeda:
#       pega
#   if troféu:
#       pula
#       break
# pega

########################################################
## Exemplo usando BREAK
############################################

n = s = 0
print('Para sair digite 999')
while True:
    n = int(input('Digite um número inteiro: '))
    if n == 999:
        break
    s += n
#print('A soma dos números é {}'.format(s))
print(f'A soma foi {s}')

###############################################
###  NOVO MÉTODO DE PRINT NA TELA
#########################################

nome = 'Eduardo'
idade = 41
salario = 1000.00

print(f'O {nome} tem {idade} anos e o salário é de R${salario:.2f}' )   