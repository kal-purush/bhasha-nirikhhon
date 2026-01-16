#Demostración Singleton, Juliana Alexandra Capador Ochoa 20152020081

class DemostracionSingleton(object): #Creación de la clase
    _instance = None #Instancia nula
    def __new__(self): #Método
        if not self._instance: #Verificación de que la instancia no está creada
            self._instance = super(DemostracionSingleton,self).__new__(self) #Creación de la instancia
            self.fruta = "manzana"
        return self._instance

x = DemostracionSingleton() 
print (x.fruta)
x.fruta = "naranja"

z = DemostracionSingleton() #Acá de llama a la instancia
print (z.fruta)

w = DemostracionSingleton()
print(w.fruta)

if (x == z): #Condicional que muestra que acceden a una misma instancia

    print("Están en la misma intancia")