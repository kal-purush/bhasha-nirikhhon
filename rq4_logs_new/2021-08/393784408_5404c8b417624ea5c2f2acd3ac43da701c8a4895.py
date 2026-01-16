#Calculadora
'''
Conceitos aplicados:
 1. Variavéis (primeiroValor, segundoValor...)
 2. Entrada de usuário (input("Digite o primeiro valor: "))
 3. Operação matemática (+ / -  *)
 4. Impressão na tela (print("A soma do valor eh: "))
 5. Atribuição de variável (soma = primeiroValor + segundoValor) 
'''


primeiroValor=int(input("Digite o primeiro valor: "))
segundoValor=int(input("Digite o segundo valor: "))
terceiroValor=int(input("Digite o terceiro valor: "))
quartoValor=int(input("Digite o quarto valor: "))

soma=primeiroValor+segundoValor
subtracao=terceiroValor-quartoValor
divisao=primeiroValor/quartoValor
produto=segundoValor*terceiroValor


print("A soma do valor eh: ",soma)
print("A subtracao do valor eh: ", subtracao)
print("A divisao do valor eh: ", divisao)
print("O produto da multiplicacao eh: ", produto)



