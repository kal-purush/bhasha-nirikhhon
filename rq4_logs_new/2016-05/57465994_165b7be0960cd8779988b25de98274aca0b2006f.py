class Persona:
    def __init__(self, nombre='Juan', edad=39, sueldo=0, trabajo=None):
        self.nombre = nombre
        self.edad = edad
        self.sueldo = sueldo
        self.trabajo = trabajo

    def apellido(self):
        return self.nombre.split()[-1]

    def darAumento(self, porcentaje):
        self.sueldo *= (1.0 + porcentaje)

    def __str__(self):
        return ('%s => Nombre: %s - Edad: %s - Trabajo: %s Sueldo: %s>' %
            (self.__class__.__name__, self.nombre,self.edad, self.trabajo, self.sueldo))

if __name__ == '__main__':
    Juan = Persona('Juan Garcia', 42, 30000, 'software')
    Susana = Persona('Susana Gomez', 45, 40000, 'hardware')
    print(Juan.nombre, Susana.sueldo)
    print(Juan.apellido())
    Susana.darAumento(0.10)
    print(Susana.sueldo)