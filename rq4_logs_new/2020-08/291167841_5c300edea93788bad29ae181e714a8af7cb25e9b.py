

def ejemplo_none(n1, n2, n3 = None):
    resultado = n1 + n2 
    if n3 is not None:
        resultado = resultado * n3

    return resultado

print(ejemplo_none(4,2,0))

