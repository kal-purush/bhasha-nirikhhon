#exercicio4: faca um programa que leia uma string e mostre ela ao contrario

def lerstring(a):
	print a [::-1]

print "digite um texto"
a = raw_input()

print lerstring(a)