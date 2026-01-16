from Persona import Persona

class Estudiante(Persona):
    def __init__(self, codigo, nombre, apellido, edad, carrera, semestre):
        super().__init__(codigo, nombre, apellido, edad)
        self.carrera = carrera
        self.semestre = semestre
    
    def __str__(self):
        return f"{super().__str__()}, Carrera: {self.carrera}, Semestre: {self.semestre}"

class GestorEstudiantes:
    def __init__(self):
        self.estudiantes = {}
    
    def agregar(self, codigo, nombre, apellido, edad, carrera, semestre):
        if codigo in self.estudiantes:
            return False
        self.estudiantes[codigo] = Estudiante(codigo, nombre, apellido, edad, carrera, semestre)
        return True
    
    def consultar(self, codigo=None):
        if codigo is None:
            return list(self.estudiantes.values())
        return self.estudiantes.get(codigo)
    
    def actualizar(self, codigo, nombre=None, apellido=None, edad=None, carrera=None, semestre=None):
        if codigo not in self.estudiantes:
            return False
        estudiante = self.estudiantes[codigo]
        if nombre:
            estudiante.nombre = nombre
        if apellido:
            estudiante.apellido = apellido
        if edad:
            estudiante.edad = edad
        if carrera:
            estudiante.carrera = carrera
        if semestre:
            estudiante.semestre = semestre
        return True
    
    def eliminar(self, codigo):
        if codigo not in self.estudiantes:
            return False
        del self.estudiantes[codigo]
        return True