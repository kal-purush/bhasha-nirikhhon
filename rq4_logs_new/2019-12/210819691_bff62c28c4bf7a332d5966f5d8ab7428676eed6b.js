class Producto {

	constructor(id, nombre, precio, cantidad, color, img){
		this.id = id;
		this.nombre = nombre;
		this.precio = precio;
		this.cantidad = cantidad;
		this.color = color;
		this.img = img;
	}

	getId(){
		return this.id;
	}

	getPrecio(){
		return this.precio;
	}

	getNombre(){
		return this.nombre;
	}

	getCantidad(){
		return this.cantidad;
	}

	getColor(){
		return this.color;
	}

	getImg(){
		return this.img;
	}

	nuevaID(numProductos){
		return ++numProductos;
	}
}