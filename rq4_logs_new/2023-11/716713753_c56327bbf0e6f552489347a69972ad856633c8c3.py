tabla = [[' ' for _ in range(6)] for _ in range(6)]

for i in range(6):
    for j in range(6):
        mensaje = f"Ingrese un valor para la posición ({i+1}, {j+1}): "
        dato = input(mensaje)
        
        while dato.upper() not in ['A', 'T', 'C', 'G']:
            print("Por favor, ingrese un valor válido (A, T, C, G).")
            dato = input(mensaje)
        
        tabla[i][j] = dato.upper()

for fila in tabla:
    print(fila)