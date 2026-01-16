'''escreva as expressoes em phyton'''
'''s1=carro s2=moto bmw s3=aviao'''
'''a) 'ot' esta em s1, s2, s3?'''
'''b) em quais das strings existem espaços em branco?'''
'''c)faca concatenação das strings s1, s2, s3 e com o resultado faça 10 cópias'''
'''d)qual o numero total de caracteres do resultado apresentado na letra c?'''
'''e)qual o caractere que se encontra no ponto medio?'''
'''f)em qual das strings dentre s1, s2, s3 não possui o caractere 'w'?'''

s1 = 'carro'
s2 = 'moto bmw'
s3 = 'avião'

testeOt = 'ot'
printTeste1 = testeOt in s1
print (printTeste1)
printTeste2 = testeOt in s2
print (printTeste2)
printTeste3 = testeOt in s3
print (printTeste3)

print ('-------------------------------')

testeEspaco = ' '
espaco1 = testeEspaco in s1
print (espaco1)
espaco2 = testeEspaco in s2
print (espaco2)
espaco3 = testeEspaco in s3
print (espaco3)

print ('-------------------------------')

copias = (s1 + s2 + s3) * 10
print (copias)
valorCop = len(copias)
ptMedio = valorCop/2
copias[ptMedio]

print ('-------------------------------')

testeW = 'w'
printTesteW1 = testeW in s1
print (printTesteW1)
printTesteW2 = testeW in s2
print (printTesteW2)
printTesteW3 = testeW in s3
print (printTesteW3)