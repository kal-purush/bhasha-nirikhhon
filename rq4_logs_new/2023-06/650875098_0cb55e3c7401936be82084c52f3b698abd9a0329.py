class Pbodega:
    id_producto=0
    nombre=''
    tipo=''
    descripcion=''
    precio=''
    stock=''
    estado=''

    def __init__(self, id_producto,nombre,tipo,descripcion,precio,stock,estado):
        self.id_producto=id_producto
        self.nombre=nombre
        self.tipo=tipo
        self.descripcion=descripcion
        self.precio=precio
        self.stock=stock
        self.estado=estado

    def pbodegaArr(self):
        return{
            'codigo':self.id_producto,
            'nombre':self.nombre,
            'tipo':self.tipo,
            'descripcion':self.descripcion,
            'precio':self.precio,
            'stock':self.stock,
            'estado':self.estado
        }
    def __str__(self):
        return   'codigo:'+self.id_producto +' nombre:'+self.nombre+'tipo:'+self.tipo+'descripcion:'+self.descripcion,+' precio:'+self.precio+' stock:'+self.stock+ ' estado:'+self.estado