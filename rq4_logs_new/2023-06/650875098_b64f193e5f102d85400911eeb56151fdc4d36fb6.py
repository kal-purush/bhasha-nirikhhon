from zeep import Client
from .bodega import Pbodega  

class Controller:
    wsdl= 'http://localhost:8080/WebApplicationBodega/WSBodega?wsdl'
    cliente = Client(wsdl)

    def mostrarTodo(self):
        listaBodega =[]
        lista = self.cliente.service.consultarProductos()
        for i in range(len(lista)):
            #id_producto,nombre,precio,stock,autor,vigencia
            producto = Pbodega(
                            lista[i]['ID_PRODUCTO'],
                            lista[i]['NOMBRE'],
                            lista[i]['TIPO'],
                            lista[i]['DESCRIPCION'],
                            lista[i]['PRECIO'],
                            lista[i]['STOCK'],
                            lista[i]['ESTADO']
                          )
  
            listaBodega.append(producto)
        return listaBodega
    
    
    def buscarUnLibro(self, cod):
        libro = self.cliente.service.ConsultarUnProducto(cod)
        return libro
    
    def actualizarStock(self, cod, stock):
        resultado = self.cliente.service.actualizarStockLibro(cod, stock)
        return resultado