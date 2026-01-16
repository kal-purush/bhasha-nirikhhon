from modularización.tienda import *


if __name__ == "__main__":

    producto1 = Producto("Mesa", 450000, 'comedor')
    producto2 = Producto("Silla", 75000, "comedor")
    producto3 = Producto("Escritorio", 150000, "oficina")
    producto4 = Producto("Repisa", 120000, "oficina")
    producto5 = Producto("Bares", 350000, "comedor")
    producto1.print_info()

    tienda1 = Tienda("Pallabelia")
    tienda1.add_product(producto1)
    tienda1.add_product(producto2)
    tienda1.add_product(producto3)
    tienda1.add_product(producto4)
    tienda1.add_product(producto5)

    tienda1.ver_inventario(tienda1)

    tienda1.inflation(20)
    tienda1.ver_inventario(tienda1)

    tienda1.set_clearance("oficina", 50)
    tienda1.ver_inventario(tienda1)

    tienda1.sell_product(1)
    tienda1.ver_inventario(tienda1)


if __name__ == "__main__":
    pass
else:

    class Producto:
        def __init__(self, name, precio, categoria):
            self.name = name
            self.precio = precio
            self.categoria = categoria

        def update_price(self, porcentaje_cambio, se_incrementa):
            if se_incrementa:
                self.precio = int(self.precio * (1 + porcentaje_cambio * 1 / 100))
            elif not se_incrementa:
                self.precio = int(self.precio * (1 - porcentaje_cambio * 1 / 100))
            else:
                print('Debe ingresar True o False en el segundo parámetro')

        def print_info(self):
            print('\nProducto:', self.name, '\nCategoria:', self.categoria, '\nPrecio:', self.precio, "\n", "-" * 50)



from modularización.productos import *

if __name__ == "__main__":
    pass
else:

    class Tienda:
        def __init__(self, name):
            self.name = name
            self.producto = [Producto("", 0, "")]

        def add_product(self, new_product):
            self.producto.append(Producto(new_product.name, new_product.precio, new_product.categoria))
            if self.producto[0].name == "" and self.producto[0].precio == 0 and self.producto[0].categoria == "":
                self.producto.pop(0)

        def sell_product(self, id):
            print("-" * 50, "\n", "Se ha vendido el producto:", self.producto[id - 1].name)
            self.producto[id - 1].print_info()
            self.producto.pop(id - 1)

        def inflation(self, percent_increase):
            for x in range(len(self.producto)):
                self.producto[x].update_price(percent_increase, True)

        def set_clearance(self, category, percent_discount):
            for x in range(len(self.producto)):
                if self.producto[x].categoria == category:
                    self.producto[x].update_price(percent_discount, False)

        def ver_inventario(self, tienda1):
            for i in range(len(tienda1.producto)):
                print("*" * 50, "\nNombre del Producto:", tienda1.producto[i].name, "\nPrecio:",
                      tienda1.producto[i].precio, "\nCategoria:", tienda1.producto[i].categoria, "\n")


