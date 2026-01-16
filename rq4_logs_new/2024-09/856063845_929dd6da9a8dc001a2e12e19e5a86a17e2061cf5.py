# Crear un diccionario con nombre, edad y carrera de un alumno
alumno = {
    "nombre": "Juan",
    "edad": 20,
    "carrera": "Ciencia de Datos"
}

# Mostrar el diccionario completo
print(alumno)

# Utilizar el método get para mostrar el valor de la clave 'nombre'
print(alumno.get("nombre"))  # Salida: Juan

# Cambiar la edad del alumno
alumno["edad"] = 21
print(alumno)

# Agregar un par clave-valor que contenga el sexo del alumno
alumno["sexo"] = "Masculino"
print(alumno)

# Usar el método pop() para remover la edad del alumno
alumno.pop("edad")
print(alumno)

# Crear un diccionario con las notas de un alumno en tres materias
notas = {
    "Programación II": 85,
    "Ciencia de Datos": 90,
    "Programación I": 88
}

# Mostrar todos los ítems del diccionario de notas
print(notas.items())

# Crear un nuevo diccionario que sea una copia del diccionario de notas
notas_copia = notas.copy()
print(notas_copia)

# Mostrar los valores del diccionario de notas
print(notas.values())

# Mostrar la longitud del diccionario de notas
print(len(notas))