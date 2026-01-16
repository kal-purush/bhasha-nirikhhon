import strings
import stringsView

maximoPronto = False

frase = input('Entre com o texto a ser tratado:\n')
'''
while not maximoPronto:
    max = stringsView.solicitarMaximo()
    maximoPronto = stringsView.checarMaximo(max)
'''
texto = strings.output(frase)
strings.writeFile( texto , strings.justificar(texto)) # max) )