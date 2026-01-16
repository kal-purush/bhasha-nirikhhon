let persona = prompt("Ingrese su nombre");
//Listado de venta
//venta
class Producto {
  constructor(nombre, precio, cantidad) {
    this.nombre = nombre;
    this.precio = precio;
    this.cantidad = cantidad;
  }
  vender() {
    if (this.cantidad > 0) {
      this.cantidad -= 1;
      alert("venta realizada");
    } else {
      alert(`${this.cantidad} productos en Stock`);
    }
  }
}

//Productos
const producto1 = new Producto("Samsung Patanlla 27", 200000, 5);
const producto2 = new Producto("Asus Patanlla 27", 400000, 3);

alert(`
Bienvenido ${persona}

Listado de productos:

Opcion 1: Producto: ${producto1.nombre}, Valor $ ${producto1.precio}, En Stock ${producto1.cantidad}
Opcion 2: Producto: ${producto2.nombre}, Valor $ ${producto2.precio}, En Stock ${producto2.cantidad}

Escriba 3 para salir
`);

let venta = Number(prompt("Ingrese una opcion"));
//Si el valor es distinto o igual a 3 sale de la aplicacion
while (venta != 3) {
  switch (venta) {
    //Venta del primer producto
    case 1:
      let cantProdc1 = Number(
        prompt(`******** Compra ******** 
      opcion 1: ${producto1.nombre}, Valor $ ${producto1.precio}, En Stock ${producto1.cantidad} 
      Ingrese la cantidad:`)
      );
      if (cantProdc1 <= producto1.cantidad) {
        let total = cantProdc1 * producto1.precio;
        let stock = producto1.cantidad - cantProdc1;
        alert(`Costo total de la venta es de ${total}, gracias por su compra`);
        producto1.vender();
        alert(`Cantidad total en bodega es de ${stock}`);
      } else if (cantProdc1 > producto1.cantidad) {
        alert(`No contamos con el Stock Suficiente`);
      } else {
        alert(`No se logro realizar la venta`);
      }
      break;
    //venta del segundo producto
    case 2:
      let cantProdc2 = Number(
        prompt(`******** Compra ******** 
      opcion 2: ${producto2.nombre}, Valor $ ${producto2.precio}, En Stock ${producto2.cantidad} 
      Ingrese la cantidad:`)
      );
      if (cantProdc2 <= producto2.cantidad) {
        total = cantProdc2 * producto2.precio;
        stock = producto2.cantidad - cantProdc2;
        alert(`Costo total de la venta es de ${total}, gracias por su compra`);
        producto2.vender();
        alert(`Cantidad total en bodega es de ${stock}`);
      } else if (cantProdc2 > producto2.cantidad) {
        alert(`No contamos con el Stock Suficiente`);
      } else {
        alert(`No se logro realizar la venta`);
      }
      break;
    default:
      alert("No selecciono una opcion");
      break;
  }
  //ciclo continuo
  venta = Number(
    prompt(`Bienvenido ${persona}

  Listado de productos:
  
  Opcion 1: Producto: ${producto1.nombre}, Valor $ ${producto1.precio}, En Stock ${producto1.cantidad}
  Opcion 2: Producto: ${producto2.nombre}, Valor $ ${producto2.precio}, En Stock ${producto2.cantidad}
  
  Escriba 3 para salir`)
  );
}