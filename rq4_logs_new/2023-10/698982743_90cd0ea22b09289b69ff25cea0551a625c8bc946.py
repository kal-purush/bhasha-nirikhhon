# crear diccionario
puntajes = {'juan': 95,'Maria': 87,'pedro':78,'ana':92}

# acceder al valor por claves

print(puntajes['Maria'])

# modificar valores
puntajes['pedro'] = 1

print(puntajes)

# agregar elemento 
puntajes['carlos'] = 55
print(puntajes)

# eliminar elemento 

del puntajes['Maria']
print(puntajes)